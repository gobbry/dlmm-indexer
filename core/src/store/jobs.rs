// The filler's work queue. Writes that advance a job stay in write.rs, inside the block's
// transaction; jobs are opened and completed by the reconciler in coverage.rs, and a backfill
// job below the lowest range is opened by the API (insert_backfill_job).

use sqlx::postgres::PgRow;
use sqlx::{PgExecutor, PgPool, Row};

use super::convert::{job_end_kind_from_database, slot_from_database, slot_to_database};
use crate::domain::error::StoreError;
use crate::domain::ids::{JobId, Slot, SlotRange};
use crate::domain::job::{ArchiveWindow, BackfillInsert, SlotRangeJob};
use crate::domain::query::RowCountMax;

// Why a job stopped; the operator reads the text and clears the column once it is resolved.
// A blocked job whose range coverage later contains (another source filled it) is still
// completed by the reconciler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobBlock {
    // The parent chain names a slot this node no longer stores: point RPC_URL at a node with
    // history, or set ARCHIVE_RPC_URL.
    MissingInStorage(Slot),
    // The node returns a block (or body) that can never be mapped, however often it is asked.
    // Only this slot is lost: the job is cut there and the rest of it reopened.
    Unmappable(Slot),
    // A job cut below the archive's bottom ends on a slot only the block above can prove, and
    // the job that would fetch that block is blocked, so it cannot arrive: clear that one first.
    EndUnproven(Slot),
    // The archive kept answering this slot with a retryable error (an epoch it did not load,
    // an empty body): load the epoch, or point ARCHIVE_RPC_URL at a server that has it.
    ArchiveUnavailable(Slot),
}

impl JobBlock {
    fn reason_text(self) -> String {
        match self {
            Self::MissingInStorage(slot) => format!("missing_in_storage:{}", slot.get()),
            Self::Unmappable(slot) => format!("unmappable:{}", slot.get()),
            Self::EndUnproven(slot) => format!("end_unproven:{}", slot.get()),
            Self::ArchiveUnavailable(slot) => format!("archive_unavailable:{}", slot.get()),
        }
    }
}

// Which open jobs a filler lane reads. The route is applied before the LIMIT, so a backlog on
// one lane never hides the other lane's jobs from it. A job no block of which has committed
// and that straddles a window edge is read by neither lane: the reconciler's next tick splits
// it (coverage.rs), and a provider walk started first would keep the archive's share.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobSelection {
    All,
    InsideArchiveWindow(ArchiveWindow),
    OutsideArchiveWindow(ArchiveWindow),
}

pub(crate) async fn read_open_jobs(
    database: &PgPool,
    job_count_max: RowCountMax,
    selection: JobSelection,
) -> Result<Vec<SlotRangeJob>, StoreError> {
    debug_assert!(job_count_max.get() > 0);
    let (window, inside) = match selection {
        JobSelection::All => (None, true),
        JobSelection::InsideArchiveWindow(window) => (Some(window), true),
        JobSelection::OutsideArchiveWindow(window) => (Some(window), false),
    };
    let bottom = window
        .map(|window| slot_to_database(window.bottom(), "archive_bottom"))
        .transpose()?;
    let top = window
        .map(|window| slot_to_database(window.top(), "archive_top"))
        .transpose()?;
    let rows = sqlx::query(
        "SELECT id, start_slot, end_slot, next_slot, end_kind::text AS end_kind
         FROM slot_range_job
         WHERE completed_at IS NULL AND blocked_reason IS NULL AND next_slot <= end_slot
           AND ($2::bigint IS NULL OR (start_slot >= $2 AND end_slot <= $3) = $4)
           AND ($2::bigint IS NULL OR next_slot > start_slot
                OR NOT ((start_slot <= $2 - 1 AND $2 - 1 < end_slot)
                        OR (start_slot <= $3 AND $3 < end_slot)))
         ORDER BY end_slot DESC
         LIMIT $1",
    )
    .bind(i64::from(job_count_max.get()))
    .bind(bottom)
    .bind(top)
    .bind(inside)
    .fetch_all(database)
    .await?;
    let jobs = rows
        .iter()
        .map(open_job_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    // Nearest the tip first: recent history is what the API is missing now.
    debug_assert!(
        jobs.windows(2)
            .all(|pair| pair[0].range.end_inclusive > pair[1].range.end_inclusive)
    );
    Ok(jobs)
}

fn open_job_from_row(row: &PgRow) -> Result<SlotRangeJob, StoreError> {
    Ok(SlotRangeJob {
        id: JobId::new(row.try_get("id")?),
        range: SlotRange {
            start: slot_from_database(row.try_get("start_slot")?, "start_slot")?,
            end_inclusive: slot_from_database(row.try_get("end_slot")?, "end_slot")?,
        },
        next_slot: slot_from_database(row.try_get("next_slot")?, "next_slot")?,
        end_kind: job_end_kind_from_database(row.try_get("end_kind")?)?,
    })
}

// Whether the cut tail below `above_cut` can no longer be proven: nothing covers the slot
// above the cut yet, and the job that owns it, the only one that would fetch the block naming
// the tail's last block, is blocked.
pub(crate) async fn read_block_above_stuck(
    database: &PgPool,
    above_cut: Slot,
) -> Result<bool, StoreError> {
    let above_cut = slot_to_database(above_cut, "above_cut")?;
    let stuck: bool = sqlx::query_scalar(
        "SELECT EXISTS (
                    SELECT 1 FROM slot_range_job j
                    WHERE j.completed_at IS NULL AND j.blocked_reason IS NOT NULL
                      AND int8range(j.start_slot, j.end_slot, '[]') @> $1::bigint)
                AND NOT EXISTS (
                    SELECT 1 FROM slot_coverage c
                    WHERE int8range(c.start_slot, c.end_slot, '[]') @> $1::bigint)",
    )
    .bind(above_cut)
    .fetch_one(database)
    .await?;
    Ok(stuck)
}

// An unmappable block is a property of that one slot, so the job ends there (the block is a
// slot of the chain, so the job now ends on a block) and the slots after it become a fresh
// job in the same transaction, keeping the old end and how it was set; any other block stops
// the whole job.
pub(crate) async fn block_job(
    database: &PgPool,
    job_id: JobId,
    block: JobBlock,
) -> Result<(), StoreError> {
    let mut transaction = database.begin().await?;
    let cut_slot = match block {
        JobBlock::Unmappable(slot) => Some(slot_to_database(slot, "unmappable_slot")?),
        _ => None,
    };
    let old_end: Option<(i64, String)> = sqlx::query_as(
        "UPDATE slot_range_job j
         SET blocked_reason = $2,
             end_slot = least(j.end_slot, coalesce($3, j.end_slot)),
             end_kind = CASE WHEN $3 < j.end_slot THEN 'block' ELSE j.end_kind END,
             updated_at = now()
         FROM (SELECT end_slot, end_kind FROM slot_range_job WHERE id = $1) old
         WHERE j.id = $1
         RETURNING old.end_slot, old.end_kind::text",
    )
    .bind(job_id.get())
    .bind(block.reason_text())
    .bind(cut_slot)
    .fetch_optional(&mut *transaction)
    .await?;
    if let (Some(cut_slot), Some((old_end_slot, old_end_kind))) = (cut_slot, old_end)
        && cut_slot < old_end_slot
    {
        sqlx::query(
            "INSERT INTO slot_range_job (start_slot, end_slot, next_slot, end_kind)
             VALUES ($1, $2, $1, $3::job_end_kind)",
        )
        .bind(cut_slot + 1)
        .bind(old_end_slot)
        .bind(old_end_kind)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

// Nothing is owed below the lowest range until a backfill job is inserted for it, so this is
// where a backfill must end. None while nothing is indexed.
pub async fn read_lowest_coverage_start<'executor>(
    executor: impl PgExecutor<'executor>,
) -> Result<Option<Slot>, StoreError> {
    let start: Option<i64> = sqlx::query_scalar("SELECT min(start_slot) FROM slot_coverage")
        .fetch_one(executor)
        .await?;
    start
        .map(|start| slot_from_database(start, "slot_coverage.start_slot"))
        .transpose()
}

// One job from `from_slot` up to the slot below the lowest range: that slot is the parent of
// the range's first block, so the job ends on a block like any reconciler hole. One statement,
// so the lowest range read is the one the job is cut to; the exclusion constraint rejects it
// when another job already owns part of the range. The API's only write: it never touches a
// swap, and the filler and reconciler treat the row like any other job.
const INSERT_BACKFILL_JOB_SQL: &str = r#"
WITH lowest AS (
    SELECT min(start_slot) AS start_slot FROM slot_coverage
),
inserted AS (
    INSERT INTO slot_range_job (start_slot, end_slot, next_slot, end_kind)
    SELECT $1, start_slot - 1, $1, 'block'
    FROM lowest
    WHERE $1 < start_slot
    RETURNING id, end_slot
)
SELECT (SELECT start_slot FROM lowest), (SELECT id FROM inserted), (SELECT end_slot FROM inserted)
"#;

pub async fn insert_backfill_job<'executor>(
    executor: impl PgExecutor<'executor>,
    from_slot: Slot,
) -> Result<BackfillInsert, StoreError> {
    let row: (Option<i64>, Option<i64>, Option<i64>) = match sqlx::query_as(INSERT_BACKFILL_JOB_SQL)
        .bind(slot_to_database(from_slot, "from_slot")?)
        .fetch_one(executor)
        .await
    {
        Ok(row) => row,
        Err(error) if is_exclusion_violation(&error) => return Ok(BackfillInsert::OverlapsJob),
        Err(error) => return Err(error.into()),
    };
    match row {
        (None, _, _) => Ok(BackfillInsert::NothingIndexedYet),
        (Some(lowest_start), None, _) => Ok(BackfillInsert::FromAtOrAboveCoverage {
            lowest_coverage_start: slot_from_database(lowest_start, "slot_coverage.start_slot")?,
        }),
        (Some(_), Some(id), Some(end_slot)) => {
            let range = SlotRange {
                start: from_slot,
                end_inclusive: slot_from_database(end_slot, "slot_range_job.end_slot")?,
            };
            debug_assert!(range.start <= range.end_inclusive);
            Ok(BackfillInsert::Inserted {
                id: JobId::new(id),
                range,
            })
        }
        // id and end_slot come from the same inserted row, so one without the other is a
        // database that answered something else.
        (Some(_), Some(_), None) => Err(StoreError::ValueOutOfRange {
            column: "slot_range_job.end_slot",
        }),
    }
}

// exclusion_violation: the range overlaps a job, open, blocked or completed.
fn is_exclusion_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "23P01")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::JobEndKind;
    use crate::store::reconcile;

    async fn insert_job(database: &PgPool, start_slot: i64, next_slot: i64) -> JobId {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO slot_range_job (start_slot, end_slot, next_slot)
             VALUES ($1, $1 + 99, $2) RETURNING id",
        )
        .bind(start_slot)
        .bind(next_slot)
        .fetch_one(database)
        .await
        .expect("job inserts");
        JobId::new(id)
    }

    async fn open_jobs(database: &PgPool) -> Vec<(JobId, u64)> {
        read_open_jobs(database, RowCountMax::new(10), JobSelection::All)
            .await
            .expect("reads")
            .iter()
            .map(|job| (job.id, job.next_slot.get()))
            .collect()
    }

    // (start_slot, end_slot, next_slot, blocked_reason) in start order.
    async fn job_rows(database: &PgPool) -> Vec<(i64, i64, i64, Option<String>)> {
        sqlx::query_as(
            "SELECT start_slot, end_slot, next_slot, blocked_reason FROM slot_range_job
             ORDER BY start_slot",
        )
        .fetch_all(database)
        .await
        .expect("reads jobs")
    }

    // Only walkable jobs are read, nearest the tip first; finished, completed and blocked
    // jobs drop out.
    #[sqlx::test(migrations = "../migrations")]
    async fn open_jobs_exclude_finished_completed_and_blocked(database: PgPool) {
        let low = insert_job(&database, 100, 100).await;
        let high = insert_job(&database, 300, 350).await;
        insert_job(&database, 500, 600).await;
        let completed = insert_job(&database, 700, 700).await;
        let blocked = insert_job(&database, 900, 900).await;
        sqlx::query("UPDATE slot_range_job SET completed_at = now() WHERE id = $1")
            .bind(completed.get())
            .execute(&database)
            .await
            .expect("completes");
        sqlx::query("UPDATE slot_range_job SET blocked_reason = 'x' WHERE id = $1")
            .bind(blocked.get())
            .execute(&database)
            .await
            .expect("blocks");
        assert_eq!(open_jobs(&database).await, vec![(high, 350), (low, 100)]);
    }

    // An unstarted job straddling the window's top waits for the reconciler's split on both
    // lanes, so the provider never starts the archive's share; once a block of it has
    // committed it stays on the provider lane.
    #[sqlx::test(migrations = "../migrations")]
    async fn unstarted_job_straddling_the_window_is_read_by_neither_lane(database: PgPool) {
        let job = insert_job(&database, 100, 100).await;
        let window = ArchiveWindow::new(Slot::new(50), Slot::new(150)).expect("window");
        let lanes = [
            JobSelection::InsideArchiveWindow(window),
            JobSelection::OutsideArchiveWindow(window),
        ];
        let mut read_ids = Vec::new();
        for selection in lanes {
            let jobs = read_open_jobs(&database, RowCountMax::new(10), selection)
                .await
                .expect("reads");
            read_ids.push(jobs.iter().map(|job| job.id).collect::<Vec<_>>());
        }
        assert_eq!(read_ids, vec![vec![], vec![]]);

        sqlx::query("UPDATE slot_range_job SET next_slot = 101 WHERE id = $1")
            .bind(job.get())
            .execute(&database)
            .await
            .expect("walked one slot");
        let provider = read_open_jobs(&database, RowCountMax::new(10), lanes[1])
            .await
            .expect("reads");
        assert_eq!(
            provider.iter().map(|job| job.id).collect::<Vec<_>>(),
            vec![job]
        );
    }

    // The operator reads the reason as text naming the slot; the job keeps its range.
    #[sqlx::test(migrations = "../migrations")]
    async fn block_job_records_the_reason_and_keeps_the_range(database: PgPool) {
        let job = insert_job(&database, 300, 350).await;
        block_job(&database, job, JobBlock::MissingInStorage(Slot::new(370)))
            .await
            .expect("blocks");
        assert_eq!(
            job_rows(&database).await,
            vec![(300, 399, 350, Some("missing_in_storage:370".to_owned()))]
        );
    }

    // An unmappable block mid-job loses only its own slot: the job ends there, the slots
    // after it are a walkable job at once, and the reconciler opens nothing more for them.
    #[sqlx::test(migrations = "../migrations")]
    async fn unmappable_block_mid_job_reopens_the_rest(database: PgPool) {
        sqlx::query(
            "INSERT INTO slot_coverage (start_slot, end_slot, end_block_time)
             VALUES (0, 99, now()), (400, 499, now())",
        )
        .execute(&database)
        .await
        .expect("seeds coverage");
        let mut connection = database.acquire().await.expect("connection");
        reconcile(&mut connection, None)
            .await
            .expect("opens the hole's job");
        let job: i64 = sqlx::query_scalar("SELECT id FROM slot_range_job")
            .fetch_one(&database)
            .await
            .expect("one job");
        sqlx::query("UPDATE slot_range_job SET next_slot = 150 WHERE id = $1")
            .bind(job)
            .execute(&database)
            .await
            .expect("walked to 150");

        block_job(
            &database,
            JobId::new(job),
            JobBlock::Unmappable(Slot::new(180)),
        )
        .await
        .expect("blocks");
        assert_eq!(
            job_rows(&database).await,
            vec![
                (100, 180, 150, Some("unmappable:180".to_owned())),
                (181, 399, 181, None),
            ]
        );
        let summary = reconcile(&mut connection, None).await.expect("reconciles");
        assert_eq!(summary.opened_job_count, 0);
        let open: Vec<(u64, u64)> =
            read_open_jobs(&database, RowCountMax::new(10), JobSelection::All)
                .await
                .expect("reads")
                .iter()
                .map(|job| (job.range.start.get(), job.range.end_inclusive.get()))
                .collect();
        assert_eq!(open, vec![(181, 399)]);
    }

    // Splitting a job cut below the archive keeps the cut on the piece that still ends there:
    // the blocked piece now ends on the unmappable block, the reopened rest on the cut.
    #[sqlx::test(migrations = "../migrations")]
    async fn unmappable_split_keeps_the_cut_on_the_reopened_rest(database: PgPool) {
        let job: i64 = sqlx::query_scalar(
            "INSERT INTO slot_range_job (start_slot, end_slot, next_slot, end_kind)
             VALUES (100, 199, 100, 'archive_lower_cut') RETURNING id",
        )
        .fetch_one(&database)
        .await
        .expect("job inserts");
        block_job(
            &database,
            JobId::new(job),
            JobBlock::Unmappable(Slot::new(150)),
        )
        .await
        .expect("blocks");
        let end_kinds: Vec<(i64, i64, String)> = sqlx::query_as(
            "SELECT start_slot, end_slot, end_kind::text FROM slot_range_job ORDER BY start_slot",
        )
        .fetch_all(&database)
        .await
        .expect("reads jobs");
        assert_eq!(
            end_kinds,
            vec![
                (100, 150, "block".to_owned()),
                (151, 199, "archive_lower_cut".to_owned()),
            ]
        );
        let open = read_open_jobs(&database, RowCountMax::new(10), JobSelection::All)
            .await
            .expect("reads");
        assert_eq!(open[0].end_kind, JobEndKind::ArchiveLowerCut);
    }

    // A backfill job needs a range to end below, and a start below that range; it ends on the
    // slot below the lowest range, and a second one over the same slots is refused rather than
    // stored twice. A refused insert leaves the table as it was.
    #[sqlx::test(migrations = "../migrations")]
    async fn backfill_job_ends_below_the_lowest_range_and_never_overlaps(database: PgPool) {
        let from_slot = Slot::new(250);
        assert_eq!(
            insert_backfill_job(&database, from_slot)
                .await
                .expect("inserts"),
            BackfillInsert::NothingIndexedYet
        );
        sqlx::query(
            "INSERT INTO slot_coverage (start_slot, end_slot, end_block_time)
             VALUES (300, 399, now()), (500, 599, now())",
        )
        .execute(&database)
        .await
        .expect("seeds coverage");
        assert_eq!(
            insert_backfill_job(&database, Slot::new(300))
                .await
                .expect("inserts"),
            BackfillInsert::FromAtOrAboveCoverage {
                lowest_coverage_start: Slot::new(300)
            }
        );
        assert_eq!(job_rows(&database).await, vec![]);

        let BackfillInsert::Inserted { range, .. } = insert_backfill_job(&database, from_slot)
            .await
            .expect("inserts")
        else {
            panic!("expected a job");
        };
        assert_eq!(
            (range.start, range.end_inclusive),
            (from_slot, Slot::new(299))
        );
        assert_eq!(
            insert_backfill_job(&database, Slot::new(200))
                .await
                .expect("inserts"),
            BackfillInsert::OverlapsJob
        );
        assert_eq!(job_rows(&database).await, vec![(250, 299, 250, None)]);
    }
}
