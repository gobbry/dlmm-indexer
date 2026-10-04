# Safety Reference — Control Flow, Assertions, Memory, Scope, Errors

Patterns and examples in **Solidity, Rust, Python, Go**.

---

## Table of Contents
1. [Control Flow](#control-flow)
2. [Assertions](#assertions)
3. [Memory](#memory)
4. [Scope and Variable Lifetime](#scope-and-variable-lifetime)
5. [Error Handling](#error-handling)
6. [Solidity-Specific Safety](#solidity-specific-safety)
7. [Rust-Specific Safety](#rust-specific-safety)
8. [Python-Specific Safety](#python-specific-safety)
9. [Go-Specific Safety](#go-specific-safety)

---

## Control Flow

### No Recursion

Recursion makes it impossible to statically prove bounded execution. Use explicit stacks or iterative algorithms.

```rust
// BAD — recursive tree traversal, unbounded stack depth
fn traverse(node: &Node) {
    traverse(&node.left);
    traverse(&node.right);
}

// GOOD — explicit stack, bounded iteration
fn traverse(root: &Node) {
    let mut stack: ArrayVec<&Node, MAX_DEPTH> = ArrayVec::new();
    stack.push(root);
    while let Some(node) = stack.pop() {
        assert!(stack.len() < MAX_DEPTH, "tree depth exceeded bound");
        if let Some(right) = &node.right { stack.push(right); }
        if let Some(left) = &node.left { stack.push(left); }
    }
}
```

```python
# BAD — recursive graph walk
def find_path(graph, node, target):
    if node == target:
        return [node]
    for neighbor in graph[node]:
        result = find_path(graph, neighbor, target)
        if result:
            return [node] + result
    return None

# GOOD — iterative BFS, bounded
def find_path(graph: dict, start: str, target: str, max_depth: int = 1000) -> list | None:
    queue: deque[tuple[str, list[str]]] = deque([(start, [start])])
    visited: set[str] = {start}
    iterations = 0
    while queue:
        iterations += 1
        assert iterations <= max_depth, f"search exceeded bound: {max_depth}"
        node, path = queue.popleft()
        if node == target:
            return path
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [neighbor]))
    return None
```

### Split Compound Boolean Conditions

Compound conditions that evaluate multiple booleans in a single `if` make it hard to tell which condition triggered. Split them into separate checks. Note: `if/else if` chains where each branch handles a distinct case are perfectly fine and often more readable than deeply nested alternatives.

```go
// BAD — compound condition, unclear which case triggered
if err != nil && !isRetryable(err) && attempts < maxAttempts {
    return err
}

// GOOD — separate checks, each condition clear
if err != nil {
    if !isRetryable(err) {
        return err
    }
    if attempts >= maxAttempts {
        return err
    }
}

// ALSO GOOD — if/else if is fine for distinct cases
if err == ErrTimeout {
    return retry(req)
} else if err == ErrNotFound {
    return fallback(req)
} else if err != nil {
    return err
}
```

```solidity
// BAD — compound require, which condition failed?
require(amount > 0 && to != address(0) && !paused, "invalid");

// GOOD — separate checks, precise revert reasons
if (amount == 0) revert ZeroAmount();
if (to == address(0)) revert ZeroAddress();
if (paused) revert MarketPaused();
```

### State Invariants Positively

```solidity
// BAD — negation, harder to reason about
if (!(index >= length)) { ... }

// GOOD — positive comparison, natural reading
if (index < length) {
    // invariant holds
} else {
    revert IndexOutOfBounds(index, length);
}
```

### Centralize Control Flow

Push `if`s up, push `for`s down. The parent owns branching; helpers compute.

```python
# BAD — helpers decide control flow internally
def process_order(order: Order) -> Result:
    validated = validate_and_maybe_reject(order)
    result = compute_and_maybe_retry(validated)
    return result

# GOOD — parent owns all branching, helpers are pure
def process_order(order: Order) -> Result:
    validation = validate(order)
    if not validation.ok:
        return handle_invalid(validation.error)

    result = compute(order)
    if result.needs_retry:
        return retry(order)

    return result
```

---

## Assertions

### Minimum Density: 2 Per Function

```solidity
function transfer(address to, uint256 amount) external {
    require(to != address(0), "zero address");
    require(amount > 0, "zero amount");

    uint256 senderBefore = balanceOf[msg.sender];
    require(senderBefore >= amount, "insufficient");

    balanceOf[msg.sender] = senderBefore - amount;
    balanceOf[to] += amount;

    // Postconditions — paired with the preconditions above.
    assert(balanceOf[msg.sender] == senderBefore - amount);
    assert(balanceOf[msg.sender] <= senderBefore); // no underflow wrap
}
```

```rust
fn process_batch(items: &[Item], buffer: &mut [u8]) -> usize {
    assert!(!items.is_empty(), "batch must not be empty");
    assert!(buffer.len() >= items.len() * ITEM_SIZE, "buffer too small");

    let mut written: usize = 0;
    for item in items {
        let encoded = item.encode();
        assert!(encoded.len() == ITEM_SIZE, "encoding size invariant");
        buffer[written..written + ITEM_SIZE].copy_from_slice(&encoded);
        written += ITEM_SIZE;
    }

    assert!(written == items.len() * ITEM_SIZE, "total written invariant");
    written
}
```

```python
def calculate_shares(
    deposit_amount: int,
    total_assets: int,
    total_shares: int,
) -> int:
    """Calculate shares to mint for a given deposit."""
    assert deposit_amount > 0, "deposit must be positive"
    assert total_assets >= 0, "total assets non-negative"
    assert total_shares >= 0, "total shares non-negative"

    if total_shares == 0:
        shares = deposit_amount
    else:
        assert total_assets > 0, "assets must be positive if shares exist"
        shares = (deposit_amount * total_shares) // total_assets

    assert shares >= 0, "shares non-negative postcondition"
    return shares
```

```go
func encodeBatch(records []Record, buf []byte) int {
	assert(len(records) > 0, "empty batch")
	assert(len(records) <= MaxBatchSize, "batch exceeds max")
	assert(len(buf) >= len(records)*RecordSize, "buffer too small")

	offset := 0
	for _, rec := range records {
		n := rec.EncodeTo(buf[offset:])
		assert(n == RecordSize, "record encode size mismatch")
		offset += n
	}

	assert(offset == len(records)*RecordSize, "total encoded size")
	return offset
}
```

### Pair Assertions

Assert the same property at write time AND read time.

```go
func writeBatch(batch []Record, w io.Writer) error {
	assert(len(batch) > 0, "empty batch")
	assert(len(batch) <= MaxBatchSize, "batch too large")
	encoded := encodeBatch(batch)
	assert(len(encoded) == len(batch)*RecordSize, "encoding size")
	_, err := w.Write(encoded)
	return err
}

// Paired with writeBatch — same invariants checked on the read side.
func readBatch(r io.Reader) ([]Record, error) {
	data, err := io.ReadAll(io.LimitReader(r, MaxBatchSize*RecordSize+1))
	if err != nil {
		return nil, err
	}
	assert(len(data)%RecordSize == 0, "corrupt batch: unaligned")
	assert(len(data)/RecordSize <= MaxBatchSize, "corrupt batch: too large")
	return decodeBatch(data)
}
```

```solidity
// Pair assertion for bonding curve: assert at WRITE (mint) and READ (quote).
function mint(uint256 shares) external returns (uint256 cost) {
    uint256 supplyBefore = totalSupply;
    cost = _computeCost(supplyBefore, shares);
    assert(cost > 0);

    totalSupply = supplyBefore + shares;
    assert(totalSupply > supplyBefore);
}

function quoteMint(uint256 shares) external view returns (uint256 cost) {
    cost = _computeCost(totalSupply, shares);
    assert(cost > 0); // paired with mint()
}
```

### Assert Positive AND Negative Space

```python
def resolve_market(outcome_index: int, total_outcomes: int) -> None:
    # Positive space — what we DO expect.
    assert isinstance(outcome_index, int), "outcome index must be int"
    assert outcome_index >= 0, "outcome index non-negative"
    assert outcome_index < total_outcomes, "outcome index in range"
    assert total_outcomes >= 2, "at least binary market"

    # Negative space — what we DON'T expect.
    assert total_outcomes <= MAX_OUTCOMES, "too many outcomes"
    assert outcome_index != total_outcomes, "index != count (off-by-one guard)"
```

### Compile-Time / Deploy-Time Assertions

```solidity
constructor() {
    assert(MAX_FEE_BPS <= 10_000);
    assert(MIN_DURATION > 0);
    assert(MAX_OUTCOMES >= 2);
    assert(PRECISION_FACTOR == 1e18);
    assert(MAX_FEE_BPS + PROTOCOL_FEE_BPS <= 10_000);
}
```

```rust
const _: () = {
    assert!(MAX_BATCH_SIZE <= BUFFER_CAPACITY);
    assert!(RING_BUFFER_SIZE.is_power_of_two());
    assert!(std::mem::size_of::<Order>() == 64);
    assert!(std::mem::align_of::<Order>() >= 8);
};
```

---

## Memory

### Static / Preallocated Memory

```rust
// BAD
fn process(events: &[Event]) -> Vec<Result> {
    let mut results = Vec::new(); // heap alloc in hot path
    // ...
}

// GOOD
struct Processor {
    results: Vec<Result>, // capacity set once in ::new()
}

impl Processor {
    fn new() -> Self {
        Self { results: Vec::with_capacity(MAX_BATCH) }
    }
    fn process(&mut self, events: &[Event]) -> &[Result] {
        assert!(events.len() <= MAX_BATCH);
        self.results.clear();
        for e in events { self.results.push(handle(e)); }
        &self.results
    }
}
```

```go
type Engine struct {
    orderBuf []Order // allocated once in NewEngine()
}

func NewEngine() *Engine {
    return &Engine{orderBuf: make([]Order, 0, MaxBatchSize)}
}

func (e *Engine) OnTick() {
    e.orderBuf = e.orderBuf[:0] // reset length, keep capacity
}
```

```python
# Preallocate numpy arrays for simulation hot paths.
# BAD
results = []
for step in range(num_steps):
    results.append(compute(step))

# GOOD
results = np.empty(num_steps, dtype=np.float64)
for step in range(num_steps):
    results[step] = compute(step)
```

---

## Scope and Variable Lifetime

### Declare at Smallest Scope

```rust
// BAD — declared 30 lines before use
fn process(data: &[u8]) -> u32 {
    let checksum: u32 = 0;
    // ... 30 lines ...
    let checksum = crc32(data); // shadowing hides dead variable
}

// GOOD
fn process(data: &[u8]) -> u32 {
    // ... 30 lines ...
    let checksum = crc32(data);
    assert!(checksum != 0, "null checksum");
    checksum
}
```

### Don't Alias / Duplicate State

```python
# BAD — snapshot goes stale
balance = account.balance
# ... mutations ...
print(balance)  # stale!

# GOOD — read from source of truth
print(account.balance)
```

---

## Error Handling

92% of catastrophic distributed system failures stem from incorrect handling of explicitly signaled non-fatal errors.

```go
// BAD
file.Close()

// GOOD
if err := file.Close(); err != nil {
    return fmt.Errorf("close file: %w", err)
}
```

```rust
// BAD — unwrap in production
let value = map.get(&key).unwrap();

// GOOD
let value = map.get(&key).ok_or(Error::KeyNotFound(key))?;
```

```python
# BAD — bare except
try:
    result = risky_operation()
except:
    pass

# GOOD — specific errors
try:
    result = risky_operation()
except ConnectionError as e:
    logger.error("connection failed: %s", e)
    raise
```

---

## Solidity-Specific Safety

- **Custom errors over strings:** cheaper, typed, informative. `revert ZeroAmount()` over `require(x > 0, "bad")`.
- **Check-Effects-Interactions (CEI):** checks first, then state mutation, then external calls. Always.
- **Explicitly sized types:** `uint256` not `uint`. `int256` not `int`.
- **Document struct packing:** comment slot layout with byte sizes.
- **Use `unchecked` only with proof:** when overflow is impossible (e.g., loop counter bounded by array length), add a comment explaining why.
- **Immutable over constant for addresses/hashes:** `immutable` is set in constructor, `constant` is a compile-time literal.

---

## Rust-Specific Safety

- `#[must_use]` on functions returning `Result` or important values.
- `debug_assert!` for expensive checks (run in tests/debug, elided in release).
- Prefer `checked_add`, `checked_mul`, `saturating_*` over raw arithmetic on untrusted inputs.
- `const _: () = assert!(...)` for compile-time invariants.
- Minimize `unsafe`. When unavoidable, `// SAFETY:` comment explaining soundness.
- Prefer `&[T]` slices over `&Vec<T>` in function signatures — accept the most general type.

---

## Python-Specific Safety

- Type hints everywhere. Run `mypy --strict`.
- `assert` for development invariants; explicit `if/raise ValueError(...)` for production input validation.
- `@dataclass(frozen=True)` for immutable value types.
- `Decimal` or scaled-integer arithmetic (multiply by 10**18) for financial math — **never `float`**.
- `numpy` with preallocated arrays for numerical hot paths.

---

## Go-Specific Safety

- Define a project-level `assert(cond bool, msg string)` that panics — Go lacks built-in assert.
- Always check errors: `if err != nil { return fmt.Errorf("context: %w", err) }`.
- `errgroup` for concurrent operations needing coordinated error handling.
- `sync.Pool` for hot-path allocations.
- Check heap escapes: `go build -gcflags="-m"`. Prefer value types and stack allocation.
