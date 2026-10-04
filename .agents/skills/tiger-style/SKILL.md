---
name: tiger-style
description: "Apply TigerStyle and NASA Power of 10 principles for writing safety-critical, high-performance code. Use this skill whenever writing code in Solidity, Rust, Python, or Go — especially for smart contracts, protocol engineering, DeFi, trading systems, onchain infrastructure, backend services, simulations, or any context where correctness, performance, and safety matter. Trigger on any coding task: writing functions, reviewing code, designing data structures, writing tests, implementing protocols, building libraries, or refactoring. Also trigger when the user mentions TigerStyle, Tiger Style, Power of 10, NASA coding rules, safety-critical code, assertion density, zero technical debt, defensive coding, or asks for code that is production-grade, bulletproof, or high-assurance. This skill should be applied as a coding philosophy overlay on top of whatever is being built."
---

# TigerStyle + NASA Power of 10: Safety-Critical Coding

This skill encodes the coding philosophy from TigerBeetle's TigerStyle and NASA's Power of 10 rules. These are not formatting preferences — they are engineering principles for writing code where correctness is non-negotiable.

The design goals, in order: **safety, performance, developer experience**. All three matter. Good style advances all three simultaneously.

## Core Philosophy

**Zero technical debt.** Do it right the first time. The second time may not come. What ships must be solid. We may lack features, but what we have meets our design goals.

**Simplicity is the hardest revision.** Simplicity is not the first attempt — it is the result of multiple passes, many sketches, and the discipline to "throw one away." The goal is to find the *super idea* that solves safety, performance, and developer experience simultaneously.

**Think upfront.** An hour of design is worth weeks in production. A problem solved in design is orders of magnitude cheaper than one solved in production. Since it's hard enough to discover showstoppers, when we do find them, we solve them — we don't allow latency spikes or exponential-complexity algorithms to slip through.

---

## The Rules

For detailed reference with examples and code patterns, read:
- `references/safety.md` — Control flow, assertions, memory, scope, error handling
- `references/performance.md` — Back-of-envelope thinking, batching, resource hierarchy, mechanical sympathy
- `references/developer-experience.md` — Naming, comments, ordering, off-by-one prevention, style-by-numbers

Below is the condensed operating checklist.

### 1. Simple, Explicit Control Flow

- No recursion. Guarantees acyclic call graphs and bounded execution.
- No `goto`, `setjmp/longjmp`, or equivalent unstructured jumps.
- Minimal, excellent abstractions only. Every abstraction is a potential leak. Use them only when they genuinely model the domain.
- Avoid deeply nested compound boolean conditions that hide branches. `if/else if` chains are fine when each branch is a distinct, readable case — but split compound conditions (multiple booleans in one `if`) into separate checks so each condition is clear.
- State invariants positively: `if (index < length)` over `if (!(index >= length))`.
- Centralize control flow: push `if`s up and `for`s down. One function owns the branching; helpers compute, they don't branch.

### 2. Put a Limit on Everything

- All loops must have a fixed upper bound. No unbounded iteration. If a loop *should* be unbounded (event loop), assert that it cannot terminate.
- All queues, buffers, and collections must have a fixed capacity. Enforce it.
- Use explicitly-sized types (`uint32`, `u32`, `int64_t`) — never architecture-dependent sizes unless required.

### 3. No Dynamic Memory After Initialization

- All memory is allocated statically at startup or during a clearly bounded init phase.
- No `malloc`/`free` (or language equivalent) in the hot path. No garbage collection pressure.
- This forces upfront design of all memory usage patterns, yielding simpler, more predictable, more performant code.
- In languages with GC (Go, Python), minimize allocations on the hot path — preallocate, pool, reuse.

### 4. Short Functions (Hard Limit)

- **70 lines maximum per function.** No exceptions. (NASA says 60, TigerStyle says 70.)
- Each function is a verifiable logical unit. If it doesn't fit on a screen, it's too long.
- Good function shape: few parameters, simple return type, meaty logic in the body.
- When splitting: keep control flow in the parent, push pure computation into helpers.

### 5. Assertion Density (Minimum 2 Per Function)

- Assert preconditions, postconditions, invariants, argument validity, return value validity.
- **Assert the positive space AND the negative space.** Where data crosses the valid/invalid boundary is where bugs hide.
- Pair assertions: for every property, find at least two different code paths to assert it (e.g., before write *and* after read).
- Split compound assertions: `assert(a); assert(b);` over `assert(a && b);` — more precise failure info.
- Assert compile-time constants and their relationships as design integrity checks.
- Assertions are not a substitute for understanding — they *encode* your understanding.

### 6. Smallest Possible Scope

- Declare variables at the innermost scope where they're needed.
- Minimize the number of variables alive at any point.
- Calculate or check values close to where they're used. Don't introduce variables before they're needed — this prevents place-of-check to place-of-use bugs.

### 7. Check Every Return Value, Validate Every Parameter

- Every non-void return value must be checked by the caller.
- Every function must validate its parameters.
- If deliberately ignoring a return value, make it explicit (cast to void, comment why).
- All errors must be handled. 92% of catastrophic distributed system failures come from incorrect handling of non-fatal errors.

### 8. Compiler Warnings at Maximum Strictness

- All compiler warnings enabled at the strictest setting from day one.
- Zero warnings policy. If the compiler or analyzer is confused, rewrite the code to be more obviously correct.
- Use static analyzers. Solidity: slither, mythril, aderyn. Rust: clippy (`-D warnings`). Python: mypy (strict mode), ruff. Go: `go vet`, staticcheck. No excuses.

### 9. Always Say Why — But Let Code Speak First

- Code should be self-documenting through clear naming, structure, and flow. If you need a comment to explain *what* the code does, the code should probably be rewritten.
- Comments explain *why* something exists or *why* a non-obvious approach was taken — not what the code does.
- Do not write excessive comments. Redundant comments bloat context, become a maintenance burden (stale comments are worse than no comments), and increase clutter and noise.
- When a comment is warranted: motivate the design decision, share the criteria so others can evaluate it.
- Tests get a description at the top: goal, methodology, what's being verified.
- Commit messages are descriptive and informative.

### 10. Zero Dependencies (Minimize Dependencies)

- Every dependency is a supply chain attack vector, a safety risk, a performance risk.
- For critical code paths, own the code. Vendor or reimplement when the dependency is simple enough.
- When dependencies are unavoidable, pin versions, audit, and understand what you're importing.

---

## Pragmatism

These are guidelines, not dogma. The majority of cases can and should adhere to these rules — they exist because they prevent real classes of bugs and performance issues. However, there will be cases where a rule cannot be applied, or where strict adherence would be counterproductive. When breaking a rule, understand *why* the rule exists, and be explicit about why the exception is justified. The goal is informed judgment, not blind compliance.

---

## Quick Application Guide

When writing code with this skill active:

1. **Before writing:** Think about the design. What are the invariants? What are the bounds? What resources are involved? Do a back-of-envelope sketch.
2. **While writing:** Assert aggressively. Keep functions short. Keep scope tight. Name things precisely. Validate inputs and outputs.
3. **After writing:** Check — does every loop have a bound? Does every function have ≥2 assertions? Are all return values checked? Are all errors handled? Is every `why` documented?

For language-specific patterns and detailed examples, consult the reference files.
