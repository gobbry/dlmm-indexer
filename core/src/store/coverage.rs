// Completeness as data: slot_coverage records what is indexed, and every job is derived from
// its holes. Nothing here trusts a writer's claim that a range is done.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgExecutor};

use super::convert::{
    count_from_database, job_end_kind_from_database, slot_from_database, slot_to_database,
    timestamp_from_unix, unix_from_timestamp,
};
use crate::domain::block::Cursor;
use crate::domain::error::StoreError;
use crate::domain::ids::{JobId, Slot, SlotRange, UnixSeconds};
use crate::domain::job::{ArchiveWindow, JobPiece, JobSplit, ReconcileSummary};

// Merges (parent_slot, slot] with every range it overlaps or touches. The new end's time is
// the stored one when a range already reached past this block, so a replay rewrites the same
// row. The insert reads the delete's output, so the deleted rows are gone before the
// exclusion constraint checks the union. The filter is written as the constraint's own range
// expression so its GiST index finds the touching rows.
const COVER_SQL: &str = r#"
WITH touching AS (
    DELETE FROM slot_coverage
    WHERE int8range(start_slot, end_slot, '[]') && int8range($1::bigint, $2::bigint + 1, '[]')
    RETURNING start_slot, end_slot, end_block_time
)
INSERT INTO slot_coverage (start_slot, end_slot, end_block_time)
SELECT least(min(start_slot), $1 + 1),
       greatest(max(end_slot), $2),
       CASE WHEN max(end_slot) > $2
            THEN (array_agg(end_block_time ORDER BY end_slot DESC))[1]
            ELSE $3::timestamptz END
FROM touching
"#;

// Holes are the gaps between consecutive ranges; history below the lowest range is owed only when a
// backfill job for it was inserted (insert_backfill_job). A hole any uncompleted job overlaps is
// already owned, blocked jobs included: a second job could not fetch what the first could not (an
// unmappable block instead splits its job when it blocks, see jobs.rs). A hole's end is a block
// (the parent of the first block of the range above); its start may be a skipped slot, which the
// filler's first-block rule accepts. A hole is cut after `$1 - 1` and after `$2`, the archive
// window's bottom and top, so no job is ever half archive, half provider. `$2` is a block, so the
// archive piece ends on one; `$1 - 1` may be skipped, so a piece ending there is recorded as an
// archive_lower_cut, and its lane waits for the archive's first block above it to prove that tail,
// wherever the archive's bottom later moves. The update cannot see the jobs this statement inserts,
// and none of them is covered anyway. `@>` is the constraint's range expression, so the completion
// check probes coverage's GiST index rather than scanning it per job.
//
// A job opened uncut (a backfill the API inserted, or any job a window that appeared or moved
// now straddles) is cut by the same pieces, as long as no block of it has committed. The job
// keeps its id and end_kind and shrinks to its last piece, so the id a requester was given
// still cancels the newest history; the other pieces are new jobs. A job already walking
// keeps its lane, whose parent chain vouches for the rest of it. The insert reads the
// update's output, so the job has shrunk before the exclusion constraint checks the other
// pieces; the completion update skips it, since one statement may not touch a row twice.
const RECONCILE_SQL: &str = r#"
WITH ranges AS (
    SELECT start_slot, lag(end_slot) OVER (ORDER BY start_slot) AS previous_end_slot
    FROM slot_coverage
),
holes AS (
    SELECT previous_end_slot + 1 AS start_slot, start_slot - 1 AS end_slot
    FROM ranges
    WHERE previous_end_slot + 1 < start_slot
),
cuts AS (
    SELECT cut_slot FROM unnest(ARRAY[$1::bigint - 1, $2::bigint]) AS c(cut_slot)
    WHERE cut_slot IS NOT NULL
),
splittable AS (
    SELECT j.id, j.start_slot, j.end_slot, j.end_kind
    FROM slot_range_job j
    WHERE j.completed_at IS NULL AND j.blocked_reason IS NULL AND j.next_slot = j.start_slot
      AND EXISTS (SELECT 1 FROM cuts c WHERE c.cut_slot >= j.start_slot AND c.cut_slot < j.end_slot)
),
spans AS (
    SELECT NULL::bigint AS job_id, start_slot, end_slot, 'block'::job_end_kind AS end_kind
    FROM holes
    UNION ALL
    SELECT id, start_slot, end_slot, end_kind FROM splittable
),
piece_starts AS (
    SELECT s.job_id, s.start_slot AS span_start_slot, s.end_slot AS span_end_slot,
           s.end_kind AS span_end_kind, s.start_slot AS piece_start_slot
    FROM spans s
    UNION ALL
    SELECT s.job_id, s.start_slot, s.end_slot, s.end_kind, c.cut_slot + 1
    FROM spans s JOIN cuts c ON c.cut_slot >= s.start_slot AND c.cut_slot < s.end_slot
),
bounded_pieces AS (
    SELECT job_id, piece_start_slot AS start_slot, span_end_slot, span_end_kind,
           coalesce(lead(piece_start_slot) OVER (PARTITION BY job_id, span_start_slot
                                                 ORDER BY piece_start_slot) - 1,
                    span_end_slot) AS end_slot
    FROM piece_starts
),
pieces AS (
    SELECT job_id, start_slot, end_slot,
           CASE WHEN end_slot = span_end_slot THEN span_end_kind
                WHEN end_slot = $1::bigint - 1 THEN 'archive_lower_cut'
                ELSE 'block' END::job_end_kind AS end_kind
    FROM bounded_pieces
),
unowned AS (
    SELECT h.start_slot, h.end_slot, h.end_kind
    FROM pieces h
    WHERE h.job_id IS NULL
      AND NOT EXISTS (
        SELECT 1 FROM slot_range_job j
        WHERE j.completed_at IS NULL
          AND int8range(j.start_slot, j.end_slot, '[]') && int8range(h.start_slot, h.end_slot, '[]')
    )
),
opened AS (
    INSERT INTO slot_range_job (start_slot, end_slot, next_slot, end_kind)
    SELECT start_slot, end_slot, start_slot, end_kind FROM unowned ORDER BY end_slot DESC
    RETURNING id
),
split_updated AS (
    UPDATE slot_range_job j
    SET start_slot = p.start_slot, next_slot = p.start_slot, updated_at = now()
    FROM splittable s JOIN pieces p ON p.job_id = s.id AND p.end_slot = s.end_slot
    WHERE j.id = s.id
      AND j.completed_at IS NULL AND j.blocked_reason IS NULL AND j.next_slot = j.start_slot
    RETURNING j.id, s.start_slot AS span_start_slot, j.start_slot, j.end_slot,
              j.end_kind
),
split_inserted AS (
    INSERT INTO slot_range_job (start_slot, end_slot, next_slot, end_kind)
    SELECT p.start_slot, p.end_slot, p.start_slot, p.end_kind
    FROM pieces p JOIN split_updated u ON u.id = p.job_id
    WHERE p.end_slot < u.start_slot
    ORDER BY p.end_slot DESC
    RETURNING id, start_slot, end_slot, end_kind
),
split_pieces AS (
    SELECT u.id AS split_job_id, u.id, u.start_slot, u.end_slot, u.end_kind
    FROM split_updated u
    UNION ALL
    SELECT u.id, i.id, i.start_slot, i.end_slot, i.end_kind
    FROM split_inserted i
    JOIN split_updated u ON i.start_slot BETWEEN u.span_start_slot AND u.end_slot
),
completed AS (
    UPDATE slot_range_job j
    SET completed_at = now(), updated_at = now()
    WHERE j.completed_at IS NULL
      AND j.id NOT IN (SELECT id FROM splittable)
      AND EXISTS (
          SELECT 1 FROM slot_coverage c
          WHERE int8range(c.start_slot, c.end_slot, '[]')
                @> int8range(j.start_slot, j.end_slot, '[]')
      )
    RETURNING j.id
),
counts AS (
    SELECT (SELECT count(*) FROM opened)    AS opened_job_count,
           (SELECT count(*) FROM completed) AS completed_job_count
)
SELECT counts.opened_job_count, counts.completed_job_count,
       p.split_job_id, p.id, p.start_slot, p.end_slot, p.end_kind::text
FROM counts LEFT JOIN split_pieces p ON true
ORDER BY p.split_job_id, p.start_slot
"#;

pub(super) async fn cover(
    connection: &mut PgConnection,
    parent_slot: Slot,
    slot: Slot,
    block_time: UnixSeconds,
) -> Result<(), StoreError> {
    debug_assert!(parent_slot < slot);
    sqlx::query(COVER_SQL)
        .bind(slot_to_database(parent_slot, "parent_slot")?)
        .bind(slot_to_database(slot, "end_slot")?)
        .bind(timestamp_from_unix(block_time, "end_block_time")?)
        .execute(connection)
        .await?;
    Ok(())
}

// None until the first block commits.
pub(crate) async fn read_cursor<'executor>(
    executor: impl PgExecutor<'executor>,
) -> Result<Option<Cursor>, StoreError> {
    let row: Option<(i64, DateTime<Utc>)> = sqlx::query_as(
        "SELECT end_slot, end_block_time FROM slot_coverage ORDER BY end_slot DESC LIMIT 1",
    )
    .fetch_optional(executor)
    .await?;
    row.map(|(slot, block_time)| {
        Ok(Cursor {
            slot: slot_from_database(slot, "slot_coverage.end_slot")?,
            block_time: unix_from_timestamp(block_time),
        })
    })
    .transpose()
}

// The first coverage range starting above `slot`: its start minus one is the parent the chain
// names for its first block, the proof of what lies below it.
pub(crate) async fn read_coverage_start_above<'executor>(
    executor: impl PgExecutor<'executor>,
    slot: Slot,
) -> Result<Option<Slot>, StoreError> {
    let start: Option<i64> =
        sqlx::query_scalar("SELECT min(start_slot) FROM slot_coverage WHERE start_slot > $1")
            .bind(slot_to_database(slot, "slot")?)
            .fetch_one(executor)
            .await?;
    start
        .map(|start| slot_from_database(start, "slot_coverage.start_slot"))
        .transpose()
}

// One row of RECONCILE_SQL: the counts, and a split piece when there is one.
type ReconcileRow = (
    i64,
    i64,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

// Holes, and jobs not yet walked, are cut at the archive window's edges when there is one.
pub async fn reconcile(
    connection: &mut PgConnection,
    archive_window: Option<ArchiveWindow>,
) -> Result<ReconcileSummary, StoreError> {
    let (bottom, top) = match archive_window {
        Some(window) => (
            Some(slot_to_database(window.bottom(), "archive_bottom")?),
            Some(slot_to_database(window.top(), "archive_top")?),
        ),
        None => (None, None),
    };
    let rows: Vec<ReconcileRow> = sqlx::query_as(RECONCILE_SQL)
        .bind(bottom)
        .bind(top)
        .fetch_all(connection)
        .await?;
    // `counts` is one row and the piece join is a LEFT JOIN, so no row is a database that
    // answered something else.
    let Some(&(opened, completed, ..)) = rows.first() else {
        return Err(StoreError::ValueOutOfRange {
            column: "opened_job_count",
        });
    };
    debug_assert!(opened >= 0);
    debug_assert!(completed >= 0);
    Ok(ReconcileSummary {
        opened_job_count: count_from_database(opened, "opened_job_count")?,
        completed_job_count: count_from_database(completed, "completed_job_count")?,
        splits: splits_from_rows(&rows)?,
    })
}

// Rows arrive ordered by the split job, then by piece start, so each job's pieces are adjacent.
fn splits_from_rows(rows: &[ReconcileRow]) -> Result<Vec<JobSplit>, StoreError> {
    let mut splits: Vec<JobSplit> = Vec::new();
    for row in rows {
        let (_, _, Some(job_id), Some(id), Some(start), Some(end), Some(end_kind)) = row else {
            continue;
        };
        let piece = JobPiece {
            id: JobId::new(*id),
            range: SlotRange {
                start: slot_from_database(*start, "slot_range_job.start_slot")?,
                end_inclusive: slot_from_database(*end, "slot_range_job.end_slot")?,
            },
            end_kind: job_end_kind_from_database(end_kind)?,
        };
        match splits.last_mut() {
            Some(split) if split.job_id.get() == *job_id => split.pieces.push(piece),
            _ => splits.push(JobSplit {
                job_id: JobId::new(*job_id),
                pieces: vec![piece],
            }),
        }
    }
    // A job is split only when a cut falls inside it, so it always leaves two pieces or more.
    debug_assert!(splits.iter().all(|split| split.pieces.len() >= 2));
    debug_assert!(splits.iter().all(|split| {
        split
            .pieces
            .windows(2)
            .all(|pair| pair[0].range.end_inclusive.get() + 1 == pair[1].range.start.get())
    }));
    Ok(splits)
}

#[cfg(test)]
mod tests {
    use sqlx::PgPool;

    use super::*;
    use crate::domain::job::JobEndKind;

    const CHAIN_FIRST_SLOT: u64 = 1_000;
    const CHAIN_SLOT_COUNT: u64 = 40;
    const CHAIN_FIRST_PARENT_SLOT: u64 = 997;
    const CHAIN_TIME_START: i64 = 1_790_000_000;

    // xorshift64: a fixed seed keeps the permutation reproducible without a dependency.
    fn next_random(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    fn shuffled<T>(mut items: Vec<T>, seed: u64) -> Vec<T> {
        let mut state = seed;
        for index in (1..items.len()).rev() {
            let other = (next_random(&mut state) % (index as u64 + 1)) as usize;
            items.swap(index, other);
        }
        items
    }

    struct ChainBlock {
        parent_slot: Slot,
        slot: Slot,
        block_time: UnixSeconds,
    }

    // About a third of the slots are skipped; each block names the previous block as parent.
    fn fake_chain() -> Vec<ChainBlock> {
        let mut state = 0x9E37_79B9_7F4A_7C15;
        let mut parent_slot = CHAIN_FIRST_PARENT_SLOT;
        let mut chain = Vec::new();
        for slot in CHAIN_FIRST_SLOT..CHAIN_FIRST_SLOT + CHAIN_SLOT_COUNT {
            let is_last = slot == CHAIN_FIRST_SLOT + CHAIN_SLOT_COUNT - 1;
            if !is_last && next_random(&mut state).is_multiple_of(3) {
                continue;
            }
            let elapsed_seconds = i64::try_from((slot - CHAIN_FIRST_SLOT) * 2 / 5).expect("fits");
            chain.push(ChainBlock {
                parent_slot: Slot::new(parent_slot),
                slot: Slot::new(slot),
                block_time: UnixSeconds::new(CHAIN_TIME_START + elapsed_seconds),
            });
            parent_slot = slot;
        }
        chain
    }

    async fn coverage_rows(connection: &mut PgConnection) -> Vec<(i64, i64, DateTime<Utc>)> {
        sqlx::query_as(
            "SELECT start_slot, end_slot, end_block_time FROM slot_coverage ORDER BY start_slot",
        )
        .fetch_all(connection)
        .await
        .expect("coverage rows")
    }

    async fn cover_all(connection: &mut PgConnection, chain: &[&ChainBlock]) -> usize {
        let mut range_count_max = 0;
        for block in chain {
            cover(connection, block.parent_slot, block.slot, block.block_time)
                .await
                .expect("covers");
            range_count_max = range_count_max.max(coverage_rows(connection).await.len());
        }
        range_count_max
    }

    // Blocks of a chain with skipped slots, written in any order, merge into one range that
    // ends on the last block with its time; writing them all again changes nothing.
    #[sqlx::test(migrations = "../migrations")]
    async fn cover_merges_any_order_into_one_range_and_replays_as_no_op(database: PgPool) {
        let chain = fake_chain();
        assert!(
            chain.len() < CHAIN_SLOT_COUNT as usize,
            "some slots skipped"
        );
        let last = chain.last().expect("chain");
        let mut connection = database.acquire().await.expect("connection");

        let first_pass = shuffled(chain.iter().collect(), 7);
        let range_count_max = cover_all(&mut connection, &first_pass).await;
        assert!(range_count_max > 1, "the order left holes to merge");
        let merged = coverage_rows(&mut connection).await;
        let expected_end_time = timestamp_from_unix(last.block_time, "time").expect("time");
        assert_eq!(
            merged,
            vec![(
                CHAIN_FIRST_PARENT_SLOT as i64 + 1,
                last.slot.get() as i64,
                expected_end_time
            )]
        );

        cover_all(&mut connection, &shuffled(chain.iter().collect(), 11)).await;
        assert_eq!(coverage_rows(&mut connection).await, merged);
    }

    async fn seed_range(connection: &mut PgConnection, start_slot: i64, end_slot: i64) {
        sqlx::query(
            "INSERT INTO slot_coverage (start_slot, end_slot, end_block_time)
             VALUES ($1, $2, to_timestamp($2))",
        )
        .bind(start_slot)
        .bind(end_slot)
        .execute(connection)
        .await
        .expect("seeds range");
    }

    async fn seed_job(
        connection: &mut PgConnection,
        start_slot: i64,
        end_slot: i64,
        blocked_reason: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO slot_range_job (start_slot, end_slot, next_slot, blocked_reason)
             VALUES ($1, $2, $1, $3)",
        )
        .bind(start_slot)
        .bind(end_slot)
        .bind(blocked_reason)
        .execute(connection)
        .await
        .expect("seeds job");
    }

    // (start_slot, end_slot, completed) in id order.
    async fn jobs(connection: &mut PgConnection) -> Vec<(i64, i64, bool)> {
        sqlx::query_as(
            "SELECT start_slot, end_slot, completed_at IS NOT NULL FROM slot_range_job
             ORDER BY id",
        )
        .fetch_all(connection)
        .await
        .expect("jobs")
    }

    // Holes between ranges get one job each, nearest the tip first, unless an open or blocked
    // job already owns them; nothing below the lowest range is owed. A job whose range
    // coverage now contains is completed. A second pass finds nothing to do.
    #[sqlx::test(migrations = "../migrations")]
    async fn reconcile_opens_unowned_holes_and_completes_covered_jobs(database: PgPool) {
        let mut connection = database.acquire().await.expect("connection");
        for (start_slot, end_slot) in [(100, 199), (300, 399), (500, 599), (700, 799)] {
            seed_range(&mut connection, start_slot, end_slot).await;
        }
        seed_job(&mut connection, 400, 499, None).await;
        seed_job(&mut connection, 600, 699, Some("missing_in_storage:650")).await;
        seed_job(&mut connection, 300, 350, None).await;

        let summary = reconcile(&mut connection, None).await.expect("reconciles");
        assert_eq!(
            summary,
            ReconcileSummary {
                opened_job_count: 1,
                completed_job_count: 1,
                splits: Vec::new(),
            }
        );
        assert_eq!(
            jobs(&mut connection).await,
            vec![
                (400, 499, false),
                (600, 699, false),
                (300, 350, true),
                (200, 299, false),
            ]
        );

        let again = reconcile(&mut connection, None).await.expect("reconciles");
        assert_eq!(again, ReconcileSummary::default());
    }

    fn window(bottom: u64, top: u64) -> Option<ArchiveWindow> {
        Some(ArchiveWindow::new(Slot::new(bottom), Slot::new(top)).expect("window"))
    }

    // (start_slot, end_slot, end_kind) of the jobs opened, in start order.
    async fn cut_jobs(connection: &mut PgConnection) -> Vec<(i64, i64, String)> {
        sqlx::query_as(
            "SELECT start_slot, end_slot, end_kind::text FROM slot_range_job ORDER BY start_slot",
        )
        .fetch_all(connection)
        .await
        .expect("jobs")
    }

    // Coverage [0, 99], [300, 399], [500, 599], [700, 799]: holes [100, 299], [400, 499] and
    // [600, 699], reconciled under the archive window [bottom, top].
    async fn reconcile_three_holes(
        connection: &mut PgConnection,
        (bottom, top): (u64, u64),
    ) -> Vec<(i64, i64, String)> {
        for (start_slot, end_slot) in [(0, 99), (300, 399), (500, 599), (700, 799)] {
            seed_range(connection, start_slot, end_slot).await;
        }
        reconcile(connection, window(bottom, top))
            .await
            .expect("reconciles");
        cut_jobs(connection).await
    }

    fn cut_job(start_slot: i64, end_slot: i64, end_kind: JobEndKind) -> (i64, i64, String) {
        (start_slot, end_slot, end_kind.as_str().to_owned())
    }

    // A hole straddling the archive's bottom is cut right before it, so history below the
    // archive's first slot leaves the archive its share; the provider piece's end may be a
    // skipped slot, and the job records that it was cut there.
    #[sqlx::test(migrations = "../migrations")]
    async fn reconcile_cuts_a_hole_at_bottom_minus_one_and_records_the_cut(database: PgPool) {
        let mut connection = database.acquire().await.expect("connection");
        let jobs = reconcile_three_holes(&mut connection, (150, 450)).await;
        assert_eq!(
            jobs[..2],
            [
                cut_job(100, 149, JobEndKind::ArchiveLowerCut),
                cut_job(150, 299, JobEndKind::Block),
            ]
        );
    }

    // A hole straddling the archive's top is cut right after it; the top is a block, so both
    // pieces end on one.
    #[sqlx::test(migrations = "../migrations")]
    async fn reconcile_cuts_a_hole_right_after_the_archive_top(database: PgPool) {
        let mut connection = database.acquire().await.expect("connection");
        let jobs = reconcile_three_holes(&mut connection, (150, 450)).await;
        assert_eq!(
            jobs[2..],
            [
                cut_job(400, 450, JobEndKind::Block),
                cut_job(451, 499, JobEndKind::Block),
                cut_job(600, 699, JobEndKind::Block),
            ]
        );
    }

    // Holes wholly on one side of the window stay whole, and so does one whose last slot is
    // the top: a cut there would leave nothing above it.
    #[sqlx::test(migrations = "../migrations")]
    async fn reconcile_leaves_holes_within_one_side_of_the_window_whole(database: PgPool) {
        let mut connection = database.acquire().await.expect("connection");
        let jobs = reconcile_three_holes(&mut connection, (100, 699)).await;
        assert_eq!(
            jobs,
            [
                cut_job(100, 299, JobEndKind::Block),
                cut_job(400, 499, JobEndKind::Block),
                cut_job(600, 699, JobEndKind::Block),
            ]
        );
    }

    // One hole spanning the whole archive window becomes three jobs: below, inside, above.
    #[sqlx::test(migrations = "../migrations")]
    async fn reconcile_cuts_a_hole_around_the_whole_archive_window(database: PgPool) {
        let mut connection = database.acquire().await.expect("connection");
        for (start_slot, end_slot) in [(300, 399), (900, 999)] {
            seed_range(&mut connection, start_slot, end_slot).await;
        }
        reconcile(&mut connection, window(500, 700))
            .await
            .expect("reconciles");
        assert_eq!(
            cut_jobs(&mut connection).await,
            [
                cut_job(400, 499, JobEndKind::ArchiveLowerCut),
                cut_job(500, 700, JobEndKind::Block),
                cut_job(701, 899, JobEndKind::Block),
            ]
        );
    }

    async fn seed_walked_job(
        connection: &mut PgConnection,
        (start_slot, end_slot, next_slot): (i64, i64, i64),
        end_kind: JobEndKind,
    ) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO slot_range_job (start_slot, end_slot, next_slot, end_kind)
             VALUES ($1, $2, $3, $4::job_end_kind)
             RETURNING id",
        )
        .bind(start_slot)
        .bind(end_slot)
        .bind(next_slot)
        .bind(end_kind.as_str())
        .fetch_one(connection)
        .await
        .expect("seeds job")
    }

    // (id, start_slot, end_slot) of every job, in start order.
    async fn job_ids(connection: &mut PgConnection) -> Vec<(i64, i64, i64)> {
        sqlx::query_as("SELECT id, start_slot, end_slot FROM slot_range_job ORDER BY start_slot")
            .fetch_all(connection)
            .await
            .expect("jobs")
    }

    fn split_ranges(summary: &ReconcileSummary) -> Vec<(u64, u64, JobEndKind)> {
        summary
            .splits
            .iter()
            .flat_map(|split| &split.pieces)
            .map(|piece| {
                let range = piece.range;
                (range.start.get(), range.end_inclusive.get(), piece.end_kind)
            })
            .collect()
    }

    // An unstarted job (a backfill inserted uncut) straddling both window edges is cut into
    // the three pieces a hole would have been, within the exclusion constraint; the job keeps
    // its id on the piece nearest the tip, and the next tick finds nothing left to split.
    #[sqlx::test(migrations = "../migrations")]
    async fn reconcile_splits_an_unstarted_job_around_the_whole_archive_window(database: PgPool) {
        let mut connection = database.acquire().await.expect("connection");
        seed_range(&mut connection, 1_000, 1_099).await;
        let job_id = seed_walked_job(&mut connection, (100, 999, 100), JobEndKind::Block).await;

        let summary = reconcile(&mut connection, window(300, 600))
            .await
            .expect("splits within the exclusion constraint");
        let expected = [
            cut_job(100, 299, JobEndKind::ArchiveLowerCut),
            cut_job(300, 600, JobEndKind::Block),
            cut_job(601, 999, JobEndKind::Block),
        ];
        assert_eq!(cut_jobs(&mut connection).await, expected);
        assert_eq!(summary.opened_job_count, 0);
        assert_eq!(
            split_ranges(&summary),
            [
                (100, 299, JobEndKind::ArchiveLowerCut),
                (300, 600, JobEndKind::Block),
                (601, 999, JobEndKind::Block),
            ]
        );
        let ids = job_ids(&mut connection).await;
        assert_eq!(ids[2], (job_id, 601, 999));
        assert!(ids[..2].iter().all(|&(id, ..)| id > job_id));
        let split_ids: Vec<i64> = summary.splits[0]
            .pieces
            .iter()
            .map(|piece| piece.id.get())
            .collect();
        assert_eq!(
            split_ids,
            ids.iter().map(|&(id, ..)| id).collect::<Vec<_>>()
        );
        assert_eq!(summary.splits[0].job_id.get(), job_id);

        let again = reconcile(&mut connection, window(300, 600))
            .await
            .expect("reconciles");
        assert_eq!(again, ReconcileSummary::default());
        assert_eq!(cut_jobs(&mut connection).await, expected);
    }

    // A job cut below an earlier bottom keeps its id and cut end when the bottom moves down
    // into it: only the new piece below the new bottom gets a cut of its own.
    #[sqlx::test(migrations = "../migrations")]
    async fn reconcile_split_keeps_the_job_end_kind_on_its_last_piece(database: PgPool) {
        let mut connection = database.acquire().await.expect("connection");
        seed_range(&mut connection, 1_000, 1_099).await;
        let job_id = seed_walked_job(
            &mut connection,
            (100, 149, 100),
            JobEndKind::ArchiveLowerCut,
        )
        .await;
        seed_walked_job(&mut connection, (150, 999, 150), JobEndKind::Block).await;

        reconcile(&mut connection, window(120, 999))
            .await
            .expect("reconciles");
        assert_eq!(
            cut_jobs(&mut connection).await,
            [
                cut_job(100, 119, JobEndKind::ArchiveLowerCut),
                cut_job(120, 149, JobEndKind::ArchiveLowerCut),
                cut_job(150, 999, JobEndKind::Block),
            ]
        );
        assert_eq!(job_ids(&mut connection).await[1], (job_id, 120, 149));
    }

    // A job with a committed block keeps its range and lane even though it straddles the
    // top: the parent chain its walk follows already vouches for the rest of it.
    #[sqlx::test(migrations = "../migrations")]
    async fn reconcile_leaves_a_started_job_whole(database: PgPool) {
        let mut connection = database.acquire().await.expect("connection");
        seed_range(&mut connection, 1_000, 1_099).await;
        seed_walked_job(&mut connection, (100, 999, 101), JobEndKind::Block).await;

        let summary = reconcile(&mut connection, window(50, 600))
            .await
            .expect("reconciles");
        assert_eq!(summary, ReconcileSummary::default());
        assert_eq!(
            cut_jobs(&mut connection).await,
            [cut_job(100, 999, JobEndKind::Block)]
        );
    }

    // Without a window there is nothing to cut at, so an unstarted job stays whole; the first
    // tick that knows the window splits it, and the job's id stays on the piece above the top.
    #[sqlx::test(migrations = "../migrations")]
    async fn reconcile_splits_a_job_once_the_window_is_known(database: PgPool) {
        let mut connection = database.acquire().await.expect("connection");
        seed_range(&mut connection, 1_000, 1_099).await;
        let job_id = seed_walked_job(&mut connection, (100, 999, 100), JobEndKind::Block).await;

        let unknown = reconcile(&mut connection, None).await.expect("reconciles");
        assert_eq!(unknown, ReconcileSummary::default());
        assert_eq!(
            cut_jobs(&mut connection).await,
            [cut_job(100, 999, JobEndKind::Block)]
        );

        reconcile(&mut connection, window(50, 600))
            .await
            .expect("reconciles");
        assert_eq!(
            cut_jobs(&mut connection).await,
            [
                cut_job(100, 600, JobEndKind::Block),
                cut_job(601, 999, JobEndKind::Block),
            ]
        );
        assert_eq!(job_ids(&mut connection).await[1], (job_id, 601, 999));
    }

    // Two jobs may never claim the same slot, even at a shared end.
    #[sqlx::test(migrations = "../migrations")]
    async fn overlapping_job_is_rejected(database: PgPool) {
        let mut connection = database.acquire().await.expect("connection");
        seed_job(&mut connection, 100, 200, None).await;
        let error = sqlx::query(
            "INSERT INTO slot_range_job (start_slot, end_slot, next_slot) VALUES (200, 300, 200)",
        )
        .execute(&mut *connection)
        .await
        .expect_err("the exclusion constraint rejects it");
        let code = error.as_database_error().and_then(|error| error.code());
        assert_eq!(code.as_deref(), Some("23P01"));
    }
}
