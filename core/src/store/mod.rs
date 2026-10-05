// Write path is crate-private so only the processor and price feed reach it; read path is public
// for the API.

mod convert;
mod coverage;
mod deltas;
mod jobs;
mod price;
mod read;
mod rebuild;
mod write;

#[cfg(test)]
mod tests;

// Public because core/tests/processor.rs drives the reconciler beside process_block.
pub use coverage::reconcile;
pub(crate) use coverage::{read_coverage_start_above, read_cursor};
pub(crate) use jobs::{JobBlock, JobSelection, block_job, read_block_above_stuck, read_open_jobs};
// The API's one write: a backfill job row, never a swap (see jobs.rs).
pub use jobs::{insert_backfill_job, read_lowest_coverage_start};
pub(crate) use price::{
    read_unpriced_minute_range, reprice_unpriced, update_token_decimals, write_prices,
};
pub use read::{
    read_health, read_pool_metadata, read_pool_volume, read_pools, read_projection_states,
    read_recent_swaps, read_swap_log_page,
};
// Public because the indexer binary's rebuild-projection subcommand runs it.
pub use rebuild::{RebuildStart, RebuildSummary, rebuild_projection};
pub(crate) use write::write_block;
