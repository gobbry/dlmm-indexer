# Meteora DLMM Swap Indexer

This small system indexes swap activity from the Meteora DLMM program on Solana mainnet.
It persists that activity idempotently in a database. An Axum REST API serves hourly and
daily volume per pool in tokens and USD.

Three services:
(1) CORE - indexer and processor
(2) API - API that serves data
(3) CLI - an agent skill and toolings that read from the API.

`task.pdf` is the requirements source of truth. The author keeps the project extremely simple
on purpose: correctness and idempotency come before feature count.

## Key references

DLMM program ID: `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`.

IDL and reference SDK: https://github.com/MeteoraAg/dlmm-sdk

Meteora docs (index at https://docs.meteora.ag/llms.txt):
DLMM developer guide - https://docs.meteora.ag/developer-guides/dlmm/index
Program events - https://docs.meteora.ag/developer-guides/dlmm/program/events
Data API - https://docs.meteora.ag/developer-guides/dlmm/api-reference/overview

## Engineering conventions

- Functional core, thin imperative shell: decisions are pure functions of plain
  data. IO lives at the edges.
- Composition over inheritance. No OOP-style hierarchies, no trait-heavy
  dependency injection.
- Newtype for every domain value (signatures, slots, pool addresses, mints,
  token amounts, timestamps). Typestate for every lifecycle the compiler can
  enforce.
- TigerStyle: see the `tiger-style` and `rust-conventions` skills.
- Invariants are `debug_assert!`. Release builds carry no asserts.
- Code explains itself. Comments say why, never what.
- `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check` must be
  clean.

## Docs rules

- `DESIGN.md`: 2 pages max, covering the nine topics (data source, redundancy, database and schema, idempotency, decoding, API design, pricing, scaling, next steps).
- Write an ADR in `docs/adr/` only when a decision is hard to reverse, surprising without
  context, AND a real trade-off. Squash conflicting ADRs instead of appending them. Format:
  [ADR-FORMAT.md](.agents/skills/grill-with-docs/ADR-FORMAT.md).
- Create `CONTEXT.md` (domain glossary) lazily, on the first resolved domain term. Format:
  [CONTEXT-FORMAT.md](.agents/skills/grill-with-docs/CONTEXT-FORMAT.md).
- Never overwrite existing docs. Extend them. Squashing an ADR is the one exception.
- Every new or changed document passes the `asd-ste100` skill before it is done. This covers
  `README.md`, `DESIGN.md`, `SCALE.md`, files under `docs/`, `archive/README.md`,
  `core/tests/fixtures/README.md`, and the local skill files. Use STE-flavored mode for prose
  documents. Use strict mode for skill instruction files. Run
  `python3 .agents/skills/asd-ste100/scripts/ste-lint.py <file>` and leave zero hard
  violations. List any justified exception in the change. Vendored skills are never edited,
  so this rule does not apply to them.

## Tests

- Every new or changed test passes the `test-audit` authoring gate. test-audit's Validation
  section names openclaw-specific tooling. Substitute `cargo test`,
  `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --check` for that tooling.
- The repo mandates two test classes:
  - Decoder tests must use real mainnet `getTransaction` JSON fixtures. The fixtures cover a
    direct swap, a swap routed through another program (e.g. Jupiter), and a
    transaction with multiple swaps.
  - An idempotency test against the real database: ingesting the same data
    twice changes nothing.

## Git

The agent never commits or pushes. Only the author does.

## Agent skills

`.agents/skills/` is the source of truth. `.claude/skills/` holds only symlinks
into it and nothing else. `skills-lock.json` records each skill's source and
hash. Keep vendored skills verbatim. Never edit them locally.

- `tiger-style`: TigerStyle and NASA Power of 10 coding philosophy (vendored).
- `rust-conventions`: this repo's Rust rules (local).
- `test-audit`: authoring gate and audit workflow for tests (vendored, openclaw).
- `grill-with-docs`: grilling session that updates `CONTEXT.md` and ADRs inline
  (vendored, mattpocock).
- `grilling`: relentless design-tree interview (vendored, mattpocock).
- `grill-me`: user-invoked entry point to `grilling` (vendored, mattpocock).
- `meteora`: Meteora's own agent skill (vendored verbatim from
  MeteoraAg/meteora-invent). Use it as the protocol reference: program IDs,
  `references/dlmm.md`, `references/data-and-apis.md`. Its ACT path (studio CLI,
  transactions) and its data-fetch advice do not apply here: this repo reads
  from Geyser and never transacts.
- `asd-ste100`: Simplified Technical English rewrite rules (vendored, danyuchn, MIT). It is
  the gate for documentation (see Docs rules). STE-flavored mode applies to prose documents:
  short sentences, active voice, one idea per sentence. Technical terms stay. Strict mode
  applies to skill instruction files.
- `metclanker`: the CLI as an agent skill. It reads the API: health, pools, volume,
  swaps, comparison against Meteora's Data API, and backfill requests. Agents use the
  subcommands with `--output json --no-input`, never `--interactive`.
- `show-me`: visual explanations as diagrams and local HTML pages under
  `show-me/` (from the author's user-level skills). `base.css` here carries the
  Meteora brand palette. Run `.agents/skills/show-me/show.sh`, not the
  user-level copy.


There is no `CLAUDE.md`.
