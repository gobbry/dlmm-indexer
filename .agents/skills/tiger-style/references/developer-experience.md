# Developer Experience Reference — Naming, Comments, Ordering, Style

Patterns and examples in **Solidity, Rust, Python, Go**.

---

## Table of Contents
1. [Naming Things](#naming-things)
2. [Comments and Documentation](#comments-and-documentation)
3. [Ordering and Structure](#ordering-and-structure)
4. [Off-By-One Prevention](#off-by-one-prevention)
5. [Style By The Numbers](#style-by-the-numbers)
6. [Dependencies and Tooling](#dependencies-and-tooling)

---

## Naming Things

### Get the Nouns and Verbs Right

Great names capture what a thing *is* or *does* and provide a crisp mental model.

```solidity
// BAD — vague
mapping(address => uint256) public data;
function doStuff(uint256 x) external;

// GOOD — domain-precise
mapping(address => uint256) public sharesOwned;
function redeemShares(uint256 shareAmount) external;
```

### Units and Qualifiers Last, Descending Significance

Most significant word first, qualifiers and units last. Groups related variables and aligns them visually.

```rust
let latency_ms_max: u64 = 100;
let latency_ms_min: u64 = 1;
let latency_ms_avg: u64 = 50;
// All latency variables group together. Compare:
let max_latency_ms: u64 = 100; // doesn't align with min or avg
```

```solidity
uint256 fee_bps_protocol;
uint256 fee_bps_builder;
uint256 fee_bps_max;
// All fee variables group together.
```

### Equal-Length Related Names

Find names with the same character count so they align in calculations and slices.

```rust
// GOOD — "source" and "target" are both 6 chars
let source_offset = 0;
let target_offset = 0;
buffer.copy(source_offset, target_offset, length);

// BAD — "src" and "dest" are 3 and 4 chars
let src_offset = 0;
let dest_offset = 0;
```

### No Abbreviations

Unless the variable is a primitive integer in a mathematical formula (matrix, sort).

```go
// BAD
func proc(ctx *Ctx, cfg *Cfg) error { ... }

// GOOD
func processOrder(context *Context, config *Config) error { ... }
```

```python
# BAD
def calc_px(q, r):
    return q * r

# GOOD
def calculate_price(quantity: int, rate: int) -> int:
    return quantity * rate
```

### Infuse Names With Meaning

Beyond descriptive — carry information about behavior or constraints.

```rust
// Boring but correct
let allocator: Allocator = ...;

// Better — tells you the allocation strategy
let arena: Allocator = ...;   // no individual frees needed
let pool: Allocator = ...;    // fixed-size block allocation
```

```solidity
// Boring
address public resolver;

// Better — tells you the resolution mechanism
address public chainlinkResolver;    // automated, objective
address public authorizedResolver;   // time-delayed, subjective
address public multisigFallback;     // emergency backstop
```

### Don't Overload Names

Don't use the same term for two different concepts.

```solidity
// BAD — "commit" means two things
market.commit();      // finalizes a trade
consensus.commit();   // two-phase commit protocol step

// GOOD — distinct terms
market.finalize();
consensus.commit();
```

### Callbacks / Hooks Go Last, Prefixed With Caller Name

```go
func readSector(disk *Disk, offset uint64, callback ReadCallback) { ... }

// Helper prefixed with caller name to show call chain
func readSector(disk *Disk, offset uint64) { ... }
func readSectorCallback(result SectorData) { ... }
```

---

## Comments and Documentation

### Code Should Explain Itself First

Good code is self-documenting through clear naming, explicit control flow, and logical structure. If you need a comment to explain *what* the code does, rewrite the code instead.

Comments exist for *why* — why something exists, why a non-obvious approach was taken, why a particular tradeoff was chosen. Do not write comments that merely restate the code. Excessive comments bloat context, become stale (a stale comment is worse than no comment), and increase noise that obscures the comments that actually matter.

### When You Do Comment, Comments Are Sentences

Well-written prose, not scribblings. Space after `//`, capital letter, full stop.

```rust
// BAD
// check balance
// update state

// GOOD
// Verify the sender has sufficient balance before debiting.
// The balance check must happen before any state mutation to prevent
// a reentrancy attack from observing inconsistent intermediate state.
```

Inline comments (end of line) can be phrases, no punctuation:

```rust
let mask = size - 1; // power-of-two fast modulo
```

### Always Say Why

Code shows *what*. Comments show *why*.

```solidity
// BAD — restates the code
// Subtract fee from amount.
uint256 net = amount - fee;

// GOOD — explains the design decision
// Fee is deducted pre-transfer rather than post-transfer because the
// recipient's hook may revert, and we need the fee secured regardless.
uint256 net = amount - fee;
```

```python
# BAD
time.sleep(1)  # sleep for 1 second

# GOOD
# Rate limit to respect the RPC provider's 10 req/sec cap.
# Sleeping 1s between batches keeps us well under the limit.
time.sleep(1)
```

### Test Descriptions

Tests should have a header explaining goal, methodology, and what invariants are verified.

```rust
/// Test that the bonding curve price increases monotonically as supply grows.
///
/// Methodology: mint N tokens in fixed increments, record the marginal cost
/// at each step, and assert strict monotonic increase. Also verify that the
/// total cost equals the integral of the curve (within rounding tolerance).
#[test]
fn test_bonding_curve_monotonic_price() { ... }
```

```python
def test_parimutuel_payout_sums_to_pool():
    """
    Verify that total payouts for the winning outcome equal the total pool.

    Methodology: create a market with 3 outcomes, place bets across all
    outcomes, resolve to outcome 0, compute each winner's payout, and
    assert that sum(payouts) == total_pool (within 1 wei rounding).
    """
```

```solidity
/// @dev Test that redeem tax is monotonically decreasing over time.
///
/// Methodology: advance block.timestamp in 1-hour increments from
/// market creation to expiry. At each step, compute the redeem tax
/// and assert it is strictly less than the previous step's tax.
function test_redeemTax_monotonicallyDecreasing() public { ... }
```

---

## Ordering and Structure

### Important Things First

Files are read top-down. Put the most important things near the top.

```solidity
// Solidity: public interface first, then internal helpers, then private.
contract Market {
    // --- Errors and Events ---
    error ZeroAmount();
    event Minted(address indexed user, uint256 shares, uint256 cost);

    // --- State ---
    uint256 public totalSupply;

    // --- Public interface ---
    function mint(uint256 shares) external returns (uint256 cost) { ... }
    function redeem(uint256 shares) external returns (uint256 payout) { ... }

    // --- Internal helpers ---
    function _computeCost(uint256 supply, uint256 shares) internal view returns (uint256) { ... }
}
```

```python
# Python module ordering:
# 1. Module docstring
# 2. Constants
# 3. Core public functions / classes
# 4. Internal helpers
# 5. __main__ block (if script)
```

```rust
// Rust: pub items first, then pub(crate), then private.
// Within each visibility level: types, then functions.
```

### Group Allocation and Deallocation

```go
file, err := os.Open(path)
if err != nil { return err }
defer file.Close()

conn, err := db.Acquire(ctx)
if err != nil { return err }
defer conn.Release()

// ... use file and conn ...
```

---

## Off-By-One Prevention

### Distinguish Index, Count, Size

These are semantically distinct even though they're all integers:
- **Index:** 0-based position. Range: `[0, count)`.
- **Count:** 1-based quantity. `count = last_index + 1`.
- **Size:** Bytes. `size = count × unit_size`.

```rust
let items_count = 10;
let item_size_bytes = 64;
let buffer_size_bytes = items_count * item_size_bytes;
let last_item_index = items_count - 1; // explicit conversion
```

Include units in names to make conversions explicit:

```python
outcomes_count = 3
outcome_index = 2        # 0-based, so max valid = outcomes_count - 1
assert outcome_index < outcomes_count, "index must be < count"
```

### Show Division Intent

```rust
// BAD — unclear rounding behavior
let pages = total_bytes / page_size;

// GOOD — explicit
let pages = div_ceil(total_bytes, page_size); // rounds up: no data loss
```

```solidity
// Solidity: be explicit about rounding direction.
// Round DOWN (favors the vault / protocol):
uint256 shares = (amount * totalShares) / totalAssets;

// Round UP (favors the user, or protects against rounding exploits):
uint256 shares = (amount * totalShares + totalAssets - 1) / totalAssets;

// Always comment which direction and why.
```

```python
# Python: use explicit floor/ceil
import math
pages = math.ceil(total_bytes / page_size)  # rounds up: no data loss
```

---

## Style By The Numbers

### Line Length: Hard Limit 100 Columns

Nothing hidden by a horizontal scrollbar. 100 columns fits two files side-by-side.

### Function Length: Hard Limit 70 Lines

If it doesn't fit on a screen, it's too long. Split: keep control flow in parent, push computation to helpers.

### Braces Always

```go
// BAD — single-line if without braces (where applicable)
if err != nil
    return err

// GOOD — always braced
if err != nil {
    return err
}
```

### Language-Specific Formatting

- **Solidity:** Follow the Solidity style guide. `forge fmt`.
- **Rust:** `cargo fmt`. Clippy with `-D warnings`.
- **Python:** `ruff format`. `ruff check`. `mypy --strict`.
- **Go:** `gofmt` (non-negotiable). `go vet`. `staticcheck`.

---

## Dependencies and Tooling

### Minimize Dependencies for Critical Paths

Every dependency is a supply chain attack surface, a performance unknown, a maintenance burden. For critical code, own it. Vendor or reimplement when simple enough.

```solidity
// Solidity: prefer vendoring over npm packages for core math.
// Copy the specific functions you need from OpenZeppelin or Solady,
// audit them, and include them directly. Don't import the entire library
// if you only need one function.
```

### Commit Messages

Descriptive, informative, in the imperative mood. Commit messages are read in `git blame` — PR descriptions are not stored in the repo.

```
# BAD
fix bug

# GOOD
fix: bonding curve underflow when supply approaches zero

The cost function T(t) * x^2 produced a zero result for x < 1e-9
due to fixed-point truncation in UD60x18.mul. This caused the
router to quote zero cost for dust amounts, allowing free minting.

Solution: add a minimum cost floor of 1 wei, enforced in
BBLBMath.cost(). Added fuzz test covering x in [0, 1e-9] range.
```
