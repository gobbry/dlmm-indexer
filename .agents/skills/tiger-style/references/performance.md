# Performance Reference — Back-of-Envelope, Batching, Mechanical Sympathy

Patterns and examples in **Solidity, Rust, Python, Go**.

---

## Table of Contents
1. [Think About Performance From the Outset](#think-about-performance-from-the-outset)
2. [Back-of-Envelope Sketches](#back-of-envelope-sketches)
3. [Resource Hierarchy](#resource-hierarchy)
4. [Batching and Amortization](#batching-and-amortization)
5. [Control Plane vs Data Plane](#control-plane-vs-data-plane)
6. [Mechanical Sympathy](#mechanical-sympathy)
7. [Be Explicit With the Compiler](#be-explicit-with-the-compiler)
8. [Solidity Gas Optimization](#solidity-gas-optimization)

---

## Think About Performance From the Outset

The best time to get 1000x wins is in the design phase — precisely when you can't measure. It's harder to fix after implementation, and gains are smaller. Have mechanical sympathy. Like a carpenter, work with the grain.

Design-phase performance decisions:
- Data structure layout (cache-friendly array vs pointer-chasing tree)
- Batch processing vs event-at-a-time
- Static allocation vs dynamic allocation
- Hot path branch-free design
- Storage layout and slot packing (Solidity)

---

## Back-of-Envelope Sketches

Before writing code, sketch against the four resources × two dimensions:

| Resource | Bandwidth | Latency |
|----------|-----------|---------|
| Network  | ~10 Gbps  | ~0.5ms RTT (datacenter), ~50ms (cross-region) |
| Disk     | ~3 GB/s (NVMe) | ~100μs (NVMe), ~5ms (HDD) |
| Memory   | ~50 GB/s  | ~100ns (DRAM), ~1ns (L1) |
| CPU      | ~1B ops/s | ~0.3ns per op |
| EVM (gas)| ~30M gas/block | ~15k gas per SSTORE, ~2.6k per SLOAD |

### Example: Orderbook Throughput

```
Target: 1M order updates/sec
Per update: ~64 bytes

Memory bandwidth:  64B × 1M = 64 MB/s     (within ~50 GB/s — fine)
L1-hot loads:      1M × 1ns = 1ms/sec      (fine)
L1-miss loads:     1M × 100ns = 100ms/sec   (too slow — must be cache-hot)

→ Decision: array-based orderbook, not tree. Keep hot levels contiguous.
```

### Example: Onchain Batch Mint Gas

```
Target: mint shares for N participants in one tx
Per participant: 1 SSTORE (20k gas new, 5k gas update) + 1 SLOAD (2.1k gas)

10 participants, new slots:  10 × 22.1k = 221k gas   (within 30M block — fine)
100 participants, new slots: 100 × 22.1k = 2.21M gas  (fine, ~7% of block)
1000 participants:           1000 × 22.1k = 22.1M gas  (73% of block — dangerous)

→ Decision: cap batch at 100. Use merkle claim pattern for >100.
```

---

## Resource Hierarchy

Optimize for the slowest resources first, after compensating for frequency:

**Network → Disk → Memory → CPU** (offchain)
**SSTORE → SLOAD → MSTORE/MLOAD → CALLDATALOAD → arithmetic** (onchain)

```
Actual cost = per-access cost × access frequency

Offchain example:
  Disk fsync:  5ms × 100/sec = 500ms/sec wall time
  Cache miss:  100ns × 10M/sec = 1000ms/sec          ← worse!

Onchain example:
  Cold SLOAD:    2100 gas × 10 reads = 21,000 gas
  Warm SLOAD:    100 gas × 100 reads = 10,000 gas     ← cheaper despite more reads
  → Decision: access storage once, cache in memory, reuse.
```

---

## Batching and Amortization

```rust
// BAD — one syscall per message
for msg in messages {
    socket.send(&msg.encode())?;
}

// GOOD — batch into one syscall
let mut batch_buf = [0u8; BATCH_BUF_SIZE];
let mut offset = 0;
for msg in messages {
    let encoded = msg.encode();
    assert!(offset + encoded.len() <= BATCH_BUF_SIZE);
    batch_buf[offset..offset + encoded.len()].copy_from_slice(&encoded);
    offset += encoded.len();
}
socket.send(&batch_buf[..offset])?;
```

```solidity
// BAD — N storage reads for sender balance
function transferBatch(address[] calldata tos, uint256[] calldata amounts) external {
    for (uint256 i = 0; i < tos.length; i++) {
        balanceOf[msg.sender] -= amounts[i]; // SLOAD+SSTORE each iteration
        balanceOf[tos[i]] += amounts[i];
    }
}

// GOOD — read sender balance once, write once
function transferBatch(address[] calldata tos, uint256[] calldata amounts) external {
    uint256 totalDebit = 0;
    for (uint256 i = 0; i < tos.length; i++) {
        totalDebit += amounts[i];
        balanceOf[tos[i]] += amounts[i];
    }
    require(balanceOf[msg.sender] >= totalDebit);
    balanceOf[msg.sender] -= totalDebit; // one SLOAD + one SSTORE for sender
}
```

```go
// BAD — one goroutine per event
for event := range ch {
    go process(event) // unbounded goroutine spawning
}

// GOOD — batch drain, bounded workers
func (s *Server) runTick() {
    batch := s.inbox.DrainUpTo(MaxBatchSize)
    assert(len(batch) <= MaxBatchSize, "drain exceeded bound")
    for _, event := range batch {
        s.process(event)
    }
}
```

---

## Control Plane vs Data Plane

Separate setup (heavy validation, rare) from the hot path (lightweight checks, frequent).

```rust
// CONTROL PLANE — heavy validation at init
fn init_engine(config: &Config) -> Engine {
    assert!(config.batch_size > 0);
    assert!(config.batch_size <= MAX_BATCH);
    assert!(config.ring_size.is_power_of_two());
    assert!(config.tick_interval_us > 0);
    // ... expensive setup ...
    Engine::new(config)
}

// DATA PLANE — lightweight assertions in hot path
#[inline]
fn on_tick(&mut self) {
    debug_assert!(self.batch.len() <= MAX_BATCH);
    // ... hot path ...
}
```

```solidity
// CONTROL PLANE — constructor validates all parameters
constructor(uint256 maxFee, uint256 minDuration, address oracle) {
    assert(maxFee <= 10_000);
    assert(minDuration > 0);
    assert(oracle != address(0));
    // Heavy setup, runs once.
}

// DATA PLANE — hot path (called per trade)
function buy(uint256 outcomeIndex, uint256 amount) external {
    // Lightweight checks only.
    if (outcomeIndex >= outcomeCount) revert InvalidOutcome();
    if (amount == 0) revert ZeroAmount();
    // ... core logic ...
}
```

---

## Mechanical Sympathy

### Let the CPU Be a Sprinter

Give large, predictable chunks of work. Don't force branch-prediction pressure, pointer chasing, or constant context-switching.

### Cache-Friendly Data Structures

```rust
// BAD — pointer-chasing, cache-unfriendly
struct OrderBook {
    levels: BTreeMap<Price, Level>, // tree nodes scattered in heap
}

// GOOD — contiguous, cache-friendly, prefetchable
struct OrderBook {
    bids: [Level; MAX_LEVELS],
    asks: [Level; MAX_LEVELS],
    bid_count: u32,
    ask_count: u32,
}
```

### Arrays Over Linked Structures

Arrays give spatial locality, prefetchability, SIMD-friendliness. For hot-path associative lookup:
- Sorted arrays + binary search (cache-friendly, SIMD-able)
- Open-addressing hash maps (flat, contiguous)
- Perfect hashing when key space is known

### Don't React Directly to External Events

Your program runs at its own pace. Accumulate events, process on your schedule. This keeps control flow under your control, enables batching, and bounds work per time period.

```go
// BAD — react to each event
func (s *Server) OnMessage(msg Message) {
    s.process(msg) // unbounded work per event
}

// GOOD — accumulate, process on tick
func (s *Server) RunTick() {
    batch := s.inbox.DrainUpTo(MaxBatchSize)
    assert(len(batch) <= MaxBatchSize, "batch bound")
    for _, msg := range batch {
        s.process(msg)
    }
}
```

---

## Be Explicit With the Compiler

### Extract Hot Loops

Pull hot loops into standalone functions with primitive arguments so the compiler can optimize freely.

```rust
// BAD — compiler must prove self.config.threshold doesn't alias
fn process_batch(&mut self) {
    for i in 0..self.batch.len() {
        if self.batch[i].value > self.config.threshold {
            self.handle(i);
        }
    }
}

// GOOD — primitive args, no aliasing concerns
fn filter_batch(batch: &[Item], threshold: u64) -> usize {
    let mut count = 0;
    for item in batch {
        if item.value > threshold {
            count += 1;
        }
    }
    count
}
```

### Pass Options Explicitly

Never rely on library defaults. Explicit options prevent latent bugs if defaults change.

```python
# BAD — relying on default timeout, encoding, etc.
response = requests.get(url)

# GOOD — explicit about everything that matters
response = requests.get(
    url,
    timeout=5.0,
    headers={"Accept": "application/json"},
    allow_redirects=False,
)
```

---

## Solidity Gas Optimization

### Storage: The Most Expensive Resource

```solidity
// BAD — repeated SLOAD
function compute() external view returns (uint256) {
    uint256 a = expensiveMapping[key]; // SLOAD
    uint256 b = expensiveMapping[key]; // SLOAD again!
    return a + b;
}

// GOOD — cache in memory
function compute() external view returns (uint256) {
    uint256 cached = expensiveMapping[key]; // one SLOAD
    return cached + cached;
}
```

### Calldata Over Memory for Read-Only Params

```solidity
// BAD — copies array to memory
function process(uint256[] memory data) external { ... }

// GOOD — reads directly from calldata, no copy
function process(uint256[] calldata data) external { ... }
```

### Unchecked Arithmetic (With Proof)

```solidity
// The loop counter i cannot overflow because it's bounded by array length,
// which itself is bounded by calldata size (max ~24KB / 32 bytes = 768).
for (uint256 i = 0; i < data.length; ) {
    // ... process data[i] ...
    unchecked { ++i; } // safe: i < data.length < type(uint256).max
}
```

### Minimize Storage Writes

```solidity
// BAD — write intermediate state
function addLiquidity(uint256 amount) external {
    totalLiquidity += amount;           // SSTORE
    userLiquidity[msg.sender] += amount; // SSTORE
    lastUpdate[msg.sender] = block.timestamp; // SSTORE
    // 3 SSTOREs = ~60k gas minimum
}

// GOOD — pack related fields, write fewer slots
struct UserInfo {
    uint128 liquidity;
    uint64 lastUpdate;
    uint64 reserved;
}
// Single SSTORE for the packed struct update.
```
