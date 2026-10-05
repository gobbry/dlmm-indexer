use crate::domain::ids::{JobId, Slot, SlotRange};

// next_slot passing range.end_inclusive only means the walk is done; the job is complete once
// the reconciler finds its range inside one coverage range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotRangeJob {
    pub id: JobId,
    pub range: SlotRange,
    pub next_slot: Slot,
    pub end_kind: JobEndKind,
}

// How the job's end was set, fixed when the reconciler cut it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobEndKind {
    // A block: a hole's end, the archive window's top, or an unmappable slot.
    Block,
    // The slot just below the archive's bottom when the hole was cut. It may be skipped, and
    // nothing in this job can prove that: the archive's first block above names the real
    // last block.
    ArchiveLowerCut,
}

impl JobEndKind {
    pub const ALL: [Self; 2] = [Self::Block, Self::ArchiveLowerCut];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::ArchiveLowerCut => "archive_lower_cut",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReconcileSummary {
    pub opened_job_count: u32,
    pub completed_job_count: u32,
    pub splits: Vec<JobSplit>,
}

// An unstarted job that straddled an archive window edge, replaced by its pieces in start order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSplit {
    pub job_id: JobId,
    pub pieces: Vec<JobPiece>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobPiece {
    pub id: JobId,
    pub range: SlotRange,
    pub end_kind: JobEndKind,
}

// What inserting a backfill job from a resolved slot came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillInsert {
    Inserted { id: JobId, range: SlotRange },
    // No range yet, so there is no lowest range for the job to end below.
    NothingIndexedYet,
    // The slot is already at or above the lowest range: nothing below it to fill.
    FromAtOrAboveCoverage { lowest_coverage_start: Slot },
    // Another job (open, blocked or completed) already owns part of the range.
    OverlapsJob,
}

// The slots the archive lane fills: from the archive's first available slot up to `top`, a
// block a week old (or the archive's newest block, when even that is older). `top` is always a
// block, so a job cut there ends on one; `bottom` is only the first slot the archive serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveWindow {
    bottom: Slot,
    top: Slot,
}

impl ArchiveWindow {
    // None when the archive holds nothing a week old.
    pub fn new(bottom: Slot, top: Slot) -> Option<Self> {
        (bottom <= top).then_some(Self { bottom, top })
    }

    pub fn bottom(self) -> Slot {
        self.bottom
    }

    pub fn top(self) -> Slot {
        self.top
    }

    // A job goes to the archive only when the archive serves all of it, so one walk never mixes
    // the two error dialects.
    pub fn serves(self, range: SlotRange) -> bool {
        debug_assert!(self.bottom <= self.top);
        range.start >= self.bottom && range.end_inclusive <= self.top
    }

    // Whether the reconciler would cut this range: a cut falls after `bottom - 1` and after
    // `top`, and only one with slots on both sides of it cuts anything.
    pub fn straddled_by(self, range: SlotRange) -> bool {
        debug_assert!(self.bottom <= self.top);
        debug_assert!(range.start <= range.end_inclusive);
        let below_bottom = self.bottom.get().checked_sub(1);
        [below_bottom, Some(self.top.get())]
            .into_iter()
            .flatten()
            .any(|cut| range.start.get() <= cut && cut < range.end_inclusive.get())
    }
}

// What every route and cut decision reads about the archive, published by the archive lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveRouting {
    // Configured, but its bounds are not read yet: no hole is cut and no job is walked, so
    // nothing the archive should fill is handed to the metered provider in the meantime.
    Pending,
    // None: no archive, or nothing in it is a week old, so the provider fills every hole.
    Ready(Option<ArchiveWindow>),
}
