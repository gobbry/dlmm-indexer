pub use crate::domain::block::LiveSource;
use crate::domain::block::{BlockOrigin, FinalizedBlock};
use crate::domain::ids::MinuteRange;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessorMessage {
    OnBlock(FinalizedBlock, BlockOrigin),
    OnPricesFilled(MinuteRange),
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillerMessage {
    OnJobsOpened,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceFeedMessage {
    Tick,
    SweepTick,
    Shutdown,
}
