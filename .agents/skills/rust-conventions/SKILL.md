---
name: rust-conventions
description: "Project Rust rules for this repo: functional core, Newtype and Typestate always, TigerStyle naming applied to Rust, debug_assert policy, and the adopted idiomatic-rust references. Use when you write or review Rust in this repo."
---

# Rust conventions

## Shape

- Use a functional core and an imperative shell. Write decisions as pure functions of plain
  data. Keep IO in a thin async shell.
- Use composition, not inheritance. Use traits only at real seams (an external data source,
  a price source, a store). Do not write trait-object hierarchies. Do not write builder
  ceremony.

## Types

- **Newtype always.** Give every domain value its own type (`Signature`, `Slot`,
  `PoolAddress`, `Mint`, `TokenAmount`, `UnixSeconds`). Never pass a raw `u64` or `String`
  across a function boundary as a domain value. Derive only the traits that the code needs.
- **Typestate always.** If the compiler can enforce a lifecycle, encode the lifecycle as
  distinct types. Each transition consumes the previous state. Then invalid transitions do
  not compile.
- **Enums instead of booleans.** Use an enum, not a boolean, for parameters and flags.
- **Immutability by default.** Treat each `mut` as a signal to look at the code twice.

## Dependencies

- **No `solana-*` crates.** Store pubkeys as `[u8; 32]` behind newtypes. Use `bs58` for
  base58. Parse events by hand from their fixed layouts. Read RPC JSON into our own `serde`
  structs. This rule covers direct dependencies only. `yellowstone-grpc-proto` pulls
  `solana-pubkey` transitively. The repo accepts that.

## Assertions

- Use `debug_assert!`, `debug_assert_eq!` and `debug_assert_ne!` for preconditions,
  postconditions and invariants. Write at least two per non-trivial function. Split
  compound assertions.
- Release builds carry no asserts. The default release profile keeps
  `debug-assertions = false`. Do not change that.
- If input validation must hold in production, return a `Result`. Do not use an assert.

## Naming

- Use TigerStyle for every domain-owned name:
  - Use `snake_case`.
  - Do not abbreviate (`signature`, not `sig`. `transaction`, not `tx`).
  - Put units and qualifiers in suffixes (`timeout_ms`, `amount_lamports`, `retry_count`,
    `batch_size_max`).
  - Give related names a shared prefix, so that they sort together.
- Make configuration identifiers mirror their environment variable names (`db_dsn`,
  `rpc_rps_max`, `RpsMax`), also where this gives an abbreviation. Use TigerStyle for all
  other names.
- Use the Rust names for standard-library and trait-mandated names (`len`, `iter`, `fmt`,
  `from_str`). If a Clippy naming lint conflicts with TigerStyle, obey the Clippy lint.

## Control flow

- Keep each function at 70 lines or fewer.
- Push `if`s up. Push `for`s down.
- Give every loop a bound.
- Handle every `Result`. If you discard a `Result`, discard it explicitly and write a
  comment that says why.

## Comments

- Write comments that say why only. Do not write doc comments that restate the signature.
- Give each test a one-line statement of the behavior that the test protects.

## Adopted idiomatic-rust references

From https://github.com/mre/idiomatic-rust:

- The Ultimate Guide to Rust Newtypes: https://www.howtocodeit.com/guides/ultimate-guide-rust-newtypes
- Pretty State Machine Patterns in Rust (2016): https://hoverbear.org/2016/10/12/rust-state-machine-pattern/
- Rust API Guidelines: https://rust-lang.github.io/api-guidelines/
- Rust Patterns: Enums Instead Of Booleans (2019): https://blakesmith.me/2019/05/07/rust-patterns-enums-instead-of-booleans.html
- Aim For Immutability in Rust (2023): https://corrode.dev/blog/immutability/

To adopt any other idiom, from that list or from a different source, ask the author first.
