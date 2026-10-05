// Replays the event log into one projection. Offline only: the indexer is stopped, so the log
// does not move under the replay and no live block adds to the table being rebuilt.
use sqlx::{PgConnection, PgPool};

use super::convert::{
    projection_state_from_database, slot_from_database, slot_to_database,
    swap_ordinal_from_database, swap_ordinal_to_database, transaction_index_from_database,
    transaction_index_to_database,
};
use super::deltas::apply_deltas;
use super::read::read_swap_log_page;
use crate::domain::error::StoreError;
use crate::domain::ids::{Slot, SwapOrdinal, TransactionIndex, UnixSeconds};
use crate::domain::projection::{LogPosition, ProjectionName, ProjectionState};
use crate::domain::query::RowCountMax;
use crate::projection::project;

const REBUILD_PAGE_ROW_COUNT_MAX: RowCountMax = RowCountMax::new(5_000);

// No swap has slot 0, so "strictly after the zero position" is the whole log.
const LOG_START: LogPosition = LogPosition {
    slot: Slot::new(0),
    transaction_index: TransactionIndex::new(0),
    swap_ordinal: SwapOrdinal::new(0),
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildStart {
    Fresh,
    // A previous run was killed mid-replay; its cursor marks the last page it committed.
    Resumed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildSummary {
    pub start: RebuildStart,
    pub row_count: u64,
    pub page_count: u64,
}

pub async fn rebuild_projection(
    database: &PgPool,
    name: ProjectionName,
) -> Result<RebuildSummary, StoreError> {
    let (start, mut after) = begin_rebuild(database, name).await?;
    // A resumed cursor's block_time is not stored, so the first page reads without the bound.
    let mut after_block_time = UnixSeconds::new(0);
    let mut summary = RebuildSummary {
        start,
        row_count: 0,
        page_count: 0,
    };
    // Ends: every page starts strictly after the previous one and the stopped log is finite.
    loop {
        let mut transaction = database.begin().await?;
        let page = read_swap_log_page(
            &mut *transaction,
            after,
            after_block_time,
            REBUILD_PAGE_ROW_COUNT_MAX,
        )
        .await?;
        let Some(last) = page.last() else {
            break;
        };
        debug_assert!(last.position > after);
        apply_deltas(&mut transaction, &project(&page), Some(name)).await?;
        write_projection_cursor(&mut transaction, name, last.position).await?;
        transaction.commit().await?;
        after = last.position;
        after_block_time = last.block_time;
        summary.row_count += page.len() as u64;
        summary.page_count += 1;
    }
    sqlx::query(
        "UPDATE projection SET state = $2::projection_state, updated_at = now() WHERE name = $1",
    )
    .bind(name.table_name())
    .bind(ProjectionState::Live.as_str())
    .execute(database)
    .await?;
    Ok(summary)
}

// One transaction: a projection is never left truncated while still marked live.
async fn begin_rebuild(
    database: &PgPool,
    name: ProjectionName,
) -> Result<(RebuildStart, LogPosition), StoreError> {
    let mut transaction = database.begin().await?;
    let row: (String, i64, i16, i16) = sqlx::query_as(
        "SELECT state::text, cursor_slot, cursor_transaction_index, cursor_swap_ordinal
         FROM projection WHERE name = $1 FOR UPDATE",
    )
    .bind(name.table_name())
    .fetch_one(&mut *transaction)
    .await?;
    let (state, slot, transaction_index, swap_ordinal) = row;
    if projection_state_from_database(&state)? == ProjectionState::Building {
        let cursor = LogPosition {
            slot: slot_from_database(slot, "projection.cursor_slot")?,
            transaction_index: transaction_index_from_database(transaction_index)?,
            swap_ordinal: swap_ordinal_from_database(swap_ordinal)?,
        };
        transaction.commit().await?;
        return Ok((RebuildStart::Resumed, cursor));
    }
    sqlx::query(
        "UPDATE projection SET state = $2::projection_state, updated_at = now() WHERE name = $1",
    )
    .bind(name.table_name())
    .bind(ProjectionState::Building.as_str())
    .execute(&mut *transaction)
    .await?;
    write_projection_cursor(&mut transaction, name, LOG_START).await?;
    sqlx::query(truncate_sql(name))
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok((RebuildStart::Fresh, LOG_START))
}

// A table name cannot be a bind parameter, so each statement is a literal.
const fn truncate_sql(name: ProjectionName) -> &'static str {
    match name {
        ProjectionName::PoolVolume1h => "TRUNCATE pool_volume_1h",
        ProjectionName::PoolVolume1d => "TRUNCATE pool_volume_1d",
        ProjectionName::PoolStats => "TRUNCATE pool_stats",
    }
}

async fn write_projection_cursor(
    connection: &mut PgConnection,
    name: ProjectionName,
    position: LogPosition,
) -> Result<(), StoreError> {
    let result = sqlx::query(
        "UPDATE projection
         SET cursor_slot = $2, cursor_transaction_index = $3, cursor_swap_ordinal = $4,
             updated_at = now()
         WHERE name = $1",
    )
    .bind(name.table_name())
    .bind(slot_to_database(position.slot, "projection.cursor_slot")?)
    .bind(transaction_index_to_database(position.transaction_index)?)
    .bind(swap_ordinal_to_database(position.swap_ordinal)?)
    .execute(connection)
    .await?;
    debug_assert_eq!(result.rows_affected(), 1);
    Ok(())
}
