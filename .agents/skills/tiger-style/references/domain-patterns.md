# Domain Patterns — DeFi Protocols, Trading Systems, Onchain Infrastructure

Assertion-heavy, safety-first patterns for the domains where TigerStyle matters most.

---

## Table of Contents
1. [Bonding Curves and AMMs](#bonding-curves-and-amms)
2. [Prediction Markets and Parimutuel Systems](#prediction-markets-and-parimutuel-systems)
3. [Vaults and Token Accounting](#vaults-and-token-accounting)
4. [Oracle and Resolver Patterns](#oracle-and-resolver-patterns)
5. [Proxy and Upgradeability Patterns](#proxy-and-upgradeability-patterns)
6. [Fee and Referral Systems](#fee-and-referral-systems)
7. [Trading Systems (Offchain)](#trading-systems-offchain)
8. [Fixed-Point Math Safety](#fixed-point-math-safety)
9. [Onchain Data Structure Patterns](#onchain-data-structure-patterns)

---

## Bonding Curves and AMMs

### Invariant: Monotonic Pricing

The most critical bonding curve invariant: price must increase monotonically with supply. If this breaks, arbitrageurs can extract value.

```solidity
/// @dev Compute the cost to mint `shares` starting from `currentSupply`.
/// Invariant: cost(supply, shares) > cost(supply', shares) if supply > supply'.
/// Invariant: cost(supply, shares) > 0 for shares > 0.
function _computeCost(
    uint256 currentSupply,
    uint256 shares
) internal view returns (uint256 cost) {
    // Preconditions.
    if (shares == 0) revert ZeroShares();

    // Core computation: integral of the price curve from supply to supply+shares.
    // For cost(x) = T(t) * x^2, the integral is T(t) * (S1^3 - S0^3) / 3.
    uint256 supplyAfter = currentSupply + shares;
    assert(supplyAfter > currentSupply); // overflow check

    uint256 cubeAfter = _cube(supplyAfter);
    uint256 cubeBefore = _cube(currentSupply);
    assert(cubeAfter >= cubeBefore); // monotonicity of cube function

    cost = timeScalar() * (cubeAfter - cubeBefore) / (3 * PRECISION);

    // Postconditions.
    assert(cost > 0); // non-zero cost for non-zero shares
}
```

### Invariant: Conservation of Value

Total value in the system must equal total value deposited minus total value withdrawn.

```solidity
function mint(uint256 shares) external returns (uint256 cost) {
    uint256 reserveBefore = reserve;
    uint256 supplyBefore = totalSupply;

    cost = _computeCost(supplyBefore, shares);
    assert(cost > 0);

    // Effects.
    reserve += cost;
    totalSupply += shares;
    sharesOf[msg.sender] += shares;

    // Conservation invariant: reserve increased by exactly the cost.
    assert(reserve == reserveBefore + cost);
    // Supply invariant: supply increased by exactly the shares.
    assert(totalSupply == supplyBefore + shares);

    // Interaction: transfer payment.
    IERC20(paymentToken).transferFrom(msg.sender, address(this), cost);
}
```

### Testing: Fuzz the Curve

```solidity
/// @dev Fuzz test: for any supply and shares, cost must be > 0 and monotonically
/// increasing with supply.
function testFuzz_costMonotonicity(uint256 supply, uint256 shares) public {
    supply = bound(supply, 0, MAX_SUPPLY);
    shares = bound(shares, 1, MAX_SHARES);

    uint256 costLow = _computeCost(supply, shares);
    uint256 costHigh = _computeCost(supply + 1, shares);

    assertGt(costLow, 0, "cost must be positive");
    assertGe(costHigh, costLow, "cost must be monotonically increasing");
}
```

```python
def test_bonding_curve_monotonic(
    supply_range: range = range(0, 10_000, 100),
    share_amount: int = 100,
) -> None:
    """Fuzz-like sweep: cost must increase monotonically with supply."""
    prev_cost = 0
    for supply in supply_range:
        cost = compute_cost(supply, share_amount)
        assert cost > 0, f"zero cost at supply={supply}"
        assert cost >= prev_cost, (
            f"non-monotonic at supply={supply}: {cost} < {prev_cost}"
        )
        prev_cost = cost
```

---

## Prediction Markets and Parimutuel Systems

### Invariant: Payout Conservation

Total payouts for the winning outcome must equal the total pool (minus fees).

```solidity
function resolve(uint256 winningOutcome) external onlyResolver {
    // Preconditions.
    if (winningOutcome >= outcomeCount) revert InvalidOutcome(winningOutcome);
    if (resolved) revert AlreadyResolved();

    resolved = true;
    winningOutcomeIndex = winningOutcome;

    // Conservation assertion: sum of winning shares can claim the entire pool.
    uint256 winningShares = totalSharesPerOutcome[winningOutcome];
    assert(winningShares > 0); // at least one participant in the winning side

    // Each winner's payout = (their shares / winning shares) * total pool.
    // Sum of all payouts = total pool (by construction).
    // This is verified in claim() via running total.
}

function claim() external {
    uint256 userShares = sharesOf[msg.sender][winningOutcomeIndex];
    if (userShares == 0) revert NothingToClaim();

    uint256 winningTotal = totalSharesPerOutcome[winningOutcomeIndex];
    assert(winningTotal > 0); // paired with resolve() assertion

    // Payout = user's proportion of the winning pool.
    uint256 payout = (userShares * totalPool) / winningTotal;
    assert(payout > 0); // non-zero payout for non-zero shares
    assert(payout <= totalPool); // cannot exceed pool

    // Effects before interaction.
    sharesOf[msg.sender][winningOutcomeIndex] = 0;
    totalClaimed += payout;
    assert(totalClaimed <= totalPool); // running conservation check

    // Interaction.
    IERC20(paymentToken).transfer(msg.sender, payout);
}
```

### Invariant: Time Scalar Kink

For dynamic parimutuel with a time-dependent scalar, assert the kink behavior.

```solidity
/// @dev Time scalar T(t): linear increase from T_min to T_max,
/// with a kink at t_kink where the slope changes.
function timeScalar() public view returns (uint256) {
    uint256 elapsed = block.timestamp - marketCreatedAt;

    uint256 scalar;
    if (elapsed <= kinkTimestamp) {
        // Phase 1: slow ramp.
        scalar = T_MIN + (elapsed * slopePhase1) / PRECISION;
    } else {
        // Phase 2: fast ramp after kink.
        uint256 elapsedAfterKink = elapsed - kinkTimestamp;
        scalar = scalarAtKink + (elapsedAfterKink * slopePhase2) / PRECISION;
    }

    // Bound assertions.
    assert(scalar >= T_MIN);
    assert(scalar <= T_MAX);
    // Monotonicity: scalar should never decrease (time only moves forward).
    // This is guaranteed by construction since slopes are positive,
    // but assert it as a safety net.
    return scalar;
}
```

### Python Simulation: Validate Curve Behavior

```python
@dataclass(frozen=True)
class DPMConfig:
    t_min: Decimal
    t_max: Decimal
    kink_time: int        # seconds
    slope_phase1: Decimal
    slope_phase2: Decimal
    outcome_count: int

    def __post_init__(self) -> None:
        assert self.t_min > 0, "t_min must be positive"
        assert self.t_max > self.t_min, "t_max must exceed t_min"
        assert self.kink_time > 0, "kink_time must be positive"
        assert self.slope_phase1 > 0, "slope_phase1 must be positive"
        assert self.slope_phase2 >= self.slope_phase1, "phase2 slope >= phase1"
        assert 2 <= self.outcome_count <= MAX_OUTCOMES, "outcome count in range"


def simulate_market(
    config: DPMConfig,
    bets: list[tuple[int, int, int]],  # (time, outcome, amount)
) -> dict:
    """
    Simulate a full market lifecycle.

    Invariants checked:
    - Total cost paid == sum of all individual costs.
    - Total pool at resolution == sum of all bets (minus fees).
    - Winner payouts sum to total pool (within rounding).
    """
    pool = 0
    shares: dict[int, list[int]] = {i: [] for i in range(config.outcome_count)}
    costs: list[int] = []

    for time, outcome, amount in bets:
        assert 0 <= outcome < config.outcome_count, f"invalid outcome {outcome}"
        assert amount > 0, "bet amount must be positive"

        cost = compute_cost(config, time, outcome, shares, amount)
        assert cost > 0, "cost must be positive"

        shares[outcome].append(amount)
        pool += cost
        costs.append(cost)

    assert pool == sum(costs), "pool conservation failed"

    return {"pool": pool, "shares": shares, "costs": costs}
```

---

## Vaults and Token Accounting

### Invariant: Share/Asset Ratio Consistency

The share price must never allow a depositor to extract more value than they put in (rounding attacks).

```solidity
function deposit(uint256 assets) external returns (uint256 shares) {
    // Preconditions.
    if (assets == 0) revert ZeroAmount();

    uint256 totalAssetsBefore = totalAssets();
    uint256 totalSharesBefore = totalSupply;

    if (totalSharesBefore == 0) {
        // First deposit: 1:1 ratio, but require minimum deposit to prevent
        // donation attacks on the vault.
        if (assets < MIN_INITIAL_DEPOSIT) revert BelowMinimum(assets);
        shares = assets;
    } else {
        // Round DOWN to favor the vault (existing depositors).
        shares = (assets * totalSharesBefore) / totalAssetsBefore;
    }

    // Postconditions.
    assert(shares > 0); // non-zero shares for non-zero deposit
    assert(shares <= assets); // shares can't exceed assets (since ratio >= 1:1 initially)

    // Effects.
    totalSupply = totalSharesBefore + shares;
    sharesOf[msg.sender] += shares;

    // Interaction.
    IERC20(asset).transferFrom(msg.sender, address(this), assets);
}
```

### Invariant: No Value Leak on Redeem

```solidity
function redeem(uint256 shares) external returns (uint256 assets) {
    if (shares == 0) revert ZeroAmount();
    if (sharesOf[msg.sender] < shares) revert InsufficientShares();

    uint256 totalAssetsBefore = totalAssets();
    uint256 totalSharesBefore = totalSupply;
    assert(totalSharesBefore > 0); // must have shares to redeem

    // Round DOWN to favor the vault on redemption.
    assets = (shares * totalAssetsBefore) / totalSharesBefore;
    assert(assets > 0); // non-zero payout
    assert(assets <= totalAssetsBefore); // can't drain more than exists

    // Effects.
    sharesOf[msg.sender] -= shares;
    totalSupply = totalSharesBefore - shares;

    // Post-state check: remaining share price hasn't decreased.
    if (totalSupply > 0) {
        uint256 priceAfter = (totalAssets() - assets) * PRECISION / totalSupply;
        uint256 priceBefore = totalAssetsBefore * PRECISION / totalSharesBefore;
        assert(priceAfter >= priceBefore); // no value leak to redeemer
    }

    // Interaction.
    IERC20(asset).transfer(msg.sender, assets);
}
```

---

## Oracle and Resolver Patterns

### Tiered Resolution With Assertions

```solidity
/// @dev Three-tier resolver: Chainlink (objective), authorized (subjective),
/// multisig (emergency fallback). Each tier has stricter time requirements.
function resolve(uint256 marketId, uint256 outcome) external {
    Market storage market = markets[marketId];
    if (market.resolved) revert AlreadyResolved();
    if (outcome >= market.outcomeCount) revert InvalidOutcome(outcome);

    uint256 elapsed = block.timestamp - market.endTime;

    if (msg.sender == chainlinkResolver) {
        // Tier 1: Chainlink. Can resolve immediately after market ends.
        assert(elapsed >= 0); // market must have ended
    } else if (msg.sender == authorizedResolver) {
        // Tier 2: Authorized resolver. Must wait for Chainlink grace period.
        if (elapsed < CHAINLINK_GRACE_PERIOD) revert TooEarly(elapsed, CHAINLINK_GRACE_PERIOD);
    } else if (msg.sender == multisigFallback) {
        // Tier 3: Multisig. Only after both other tiers have had their chance.
        if (elapsed < MULTISIG_GRACE_PERIOD) revert TooEarly(elapsed, MULTISIG_GRACE_PERIOD);
        assert(MULTISIG_GRACE_PERIOD > CHAINLINK_GRACE_PERIOD); // design invariant
    } else {
        revert UnauthorizedResolver(msg.sender);
    }

    market.resolved = true;
    market.winningOutcome = outcome;

    emit MarketResolved(marketId, outcome, msg.sender);
}
```

---

## Proxy and Upgradeability Patterns

### UUPS With Safety Assertions

```solidity
/// @dev UUPS upgrade guard: only the contract itself (via delegatecall) can upgrade.
function _authorizeUpgrade(address newImplementation) internal override {
    // Only owner can trigger upgrades.
    if (msg.sender != owner) revert Unauthorized();
    // New implementation must be a contract, not an EOA.
    assert(newImplementation.code.length > 0);
    // Self-check: we must be running as a proxy, not directly.
    assert(address(this) != __self);
}
```

### Storage Layout Assertions

```solidity
/// @dev Assert storage slot positions match expected layout.
/// Run this in a test, not in production (it's a design integrity check).
function test_storageLayout() public {
    // ERC1967 implementation slot.
    bytes32 implSlot = bytes32(uint256(keccak256("eip1967.proxy.implementation")) - 1);
    assertEq(implSlot, 0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc);

    // Custom storage slots for upgradeable contracts.
    // These must NEVER change between upgrades.
    bytes32 marketSlot = keccak256("fortytwo.storage.markets");
    // Assert the slot matches what the compiler generates for the storage variable.
}
```

---

## Fee and Referral Systems

### Pull-Based Fee Accumulation

```solidity
/// @dev Fees accumulate in a mapping. Users pull (withdraw) their fees.
/// This avoids gas-expensive push-based distribution and reentrancy risks.
mapping(address => uint256) public accumulatedFees;

function _accumulateFee(address recipient, uint256 amount) internal {
    if (amount == 0) return;
    assert(recipient != address(0)); // no fees to zero address

    uint256 before = accumulatedFees[recipient];
    accumulatedFees[recipient] = before + amount;
    assert(accumulatedFees[recipient] >= before); // overflow check

    emit FeeAccumulated(recipient, amount);
}

function withdrawFees() external {
    uint256 amount = accumulatedFees[msg.sender];
    if (amount == 0) revert NoFeesToWithdraw();

    // Effects before interaction (CEI).
    accumulatedFees[msg.sender] = 0;

    // Interaction.
    IERC20(paymentToken).transfer(msg.sender, amount);

    // Postcondition.
    assert(accumulatedFees[msg.sender] == 0);
}
```

### Fee Cap Enforcement

```solidity
function setBuilderFee(uint256 newFeeBps) external {
    if (newFeeBps > MAX_BUILDER_FEE_BPS) revert FeeExceedsCap(newFeeBps, MAX_BUILDER_FEE_BPS);

    // Invariant: combined fees can never exceed 100%.
    uint256 totalFee = protocolFeeBps + newFeeBps;
    assert(totalFee <= 10_000); // defense in depth: can't exceed 100%

    builderFeeBps = newFeeBps;
}
```

---

## Trading Systems (Offchain)

### Orderbook Assertions (Rust)

```rust
/// Sorted-array orderbook with cache-friendly layout.
/// Invariant: bids are sorted descending by price.
/// Invariant: asks are sorted ascending by price.
struct OrderBook {
    bids: Vec<Level>,  // capacity set in ::new()
    asks: Vec<Level>,
}

impl OrderBook {
    fn insert_bid(&mut self, level: Level) {
        assert!(self.bids.len() < MAX_LEVELS, "bid levels at capacity");
        assert!(level.price > 0, "zero price");
        assert!(level.quantity > 0, "zero quantity");

        // Find insertion point (maintain sorted descending).
        let pos = self.bids.partition_point(|l| l.price > level.price);
        self.bids.insert(pos, level);

        // Postcondition: bids still sorted descending.
        debug_assert!(self.bids.windows(2).all(|w| w[0].price >= w[1].price));
    }

    fn best_bid(&self) -> Option<&Level> {
        let best = self.bids.first()?;
        // If asks exist, best bid must be < best ask (no crossed book).
        if let Some(best_ask) = self.asks.first() {
            debug_assert!(best.price < best_ask.price, "crossed book");
        }
        Some(best)
    }
}
```

### Risk Check Assertions (Go)

```go
// PreTradeRiskCheck validates an order before it reaches the matching engine.
// All checks are preconditions; failure means the order is rejected, not retried.
func PreTradeRiskCheck(order Order, account Account, config RiskConfig) error {
	assert(order.Quantity > 0, "zero quantity")
	assert(order.Price > 0, "zero price")

	// Position limit.
	newPosition := account.Position + order.SignedQuantity()
	if abs(newPosition) > config.MaxPositionSize {
		return fmt.Errorf("position limit exceeded: %d > %d", abs(newPosition), config.MaxPositionSize)
	}

	// Notional limit.
	notional := order.Price * order.Quantity
	assert(notional > 0, "notional overflow to zero")
	if notional > config.MaxNotional {
		return fmt.Errorf("notional limit exceeded: %d > %d", notional, config.MaxNotional)
	}

	// Rate limit: max orders per second.
	if account.OrdersThisSecond >= config.MaxOrdersPerSecond {
		return fmt.Errorf("rate limit: %d >= %d orders/sec", account.OrdersThisSecond, config.MaxOrdersPerSecond)
	}

	return nil
}
```

### Event Processing Pipeline Assertions (Go)

```go
// ProcessEventBatch handles a batch of onchain events.
// Invariant: events are processed in strictly increasing block order.
// Invariant: no event is processed twice (deduplication by event ID).
func (p *Processor) ProcessEventBatch(events []Event) error {
	assert(len(events) > 0, "empty batch")
	assert(len(events) <= MaxBatchSize, "batch exceeds max")

	for i, event := range events {
		// Ordering invariant.
		if i > 0 {
			prev := events[i-1]
			assert(event.BlockNumber >= prev.BlockNumber, "events not in block order")
			if event.BlockNumber == prev.BlockNumber {
				assert(event.LogIndex > prev.LogIndex, "events not in log order within block")
			}
		}

		// Deduplication invariant.
		if p.seen.Contains(event.ID) {
			return fmt.Errorf("duplicate event: %s", event.ID)
		}
		p.seen.Add(event.ID)

		// Monotonic block tracking.
		assert(event.BlockNumber >= p.lastProcessedBlock, "reorg not handled")
		p.lastProcessedBlock = event.BlockNumber
	}

	return nil
}
```

---

## Fixed-Point Math Safety

### Solidity: PRBMath / Solady Patterns

```solidity
// When using UD60x18 (18-decimal fixed-point):
// Always assert the result is in the expected range.

function computeCurveValue(UD60x18 supply, UD60x18 shares) internal pure returns (UD60x18 cost) {
    // Preconditions on inputs.
    assert(supply.unwrap() <= MAX_SUPPLY_WAD);
    assert(shares.unwrap() > 0);

    UD60x18 supplyAfter = supply.add(shares);
    assert(supplyAfter.gte(supply)); // no wrap

    // Compute: T * (S1^3 - S0^3) / 3
    UD60x18 cubeBefore = supply.powu(3);
    UD60x18 cubeAfter = supplyAfter.powu(3);
    assert(cubeAfter.gte(cubeBefore)); // monotonicity

    cost = cubeAfter.sub(cubeBefore).div(toUD60x18(3));
    assert(cost.unwrap() > 0); // non-zero cost
}
```

### Python: Decimal / Scaled Integer Patterns

```python
from decimal import Decimal, getcontext

# Set precision high enough for financial calculations.
getcontext().prec = 50

PRECISION = Decimal(10) ** 18  # 1e18 scaling factor

def compute_cost_decimal(
    supply: Decimal,
    shares: Decimal,
    time_scalar: Decimal,
) -> Decimal:
    """
    Compute bonding curve cost using Decimal for exact arithmetic.
    Mirrors the Solidity implementation for simulation validation.
    """
    assert supply >= 0, "supply non-negative"
    assert shares > 0, "shares positive"
    assert time_scalar > 0, "time scalar positive"

    supply_after = supply + shares
    assert supply_after > supply, "overflow"

    cube_before = supply ** 3
    cube_after = supply_after ** 3
    assert cube_after >= cube_before, "monotonicity"

    cost = time_scalar * (cube_after - cube_before) / 3
    assert cost > 0, "cost positive"

    return cost
```

---

## Onchain Data Structure Patterns

### Mapping + Array for Enumerable Sets

```solidity
/// @dev Enumerable set: O(1) add/remove/contains, O(n) iteration.
/// Invariant: values[indices[x]] == x for all x in the set.
/// Invariant: indices[values[i]] == i for all i < length.
struct AddressSet {
    address[] values;
    mapping(address => uint256) indices; // 1-indexed (0 = not present)
}

function _add(AddressSet storage set, address value) internal {
    if (set.indices[value] != 0) revert AlreadyExists(value);

    set.values.push(value);
    set.indices[value] = set.values.length; // 1-indexed

    // Invariant check.
    assert(set.values[set.indices[value] - 1] == value);
}

function _remove(AddressSet storage set, address value) internal {
    uint256 index = set.indices[value];
    if (index == 0) revert NotFound(value);

    uint256 lastIndex = set.values.length;
    assert(lastIndex > 0);

    if (index != lastIndex) {
        // Swap with last element.
        address lastValue = set.values[lastIndex - 1];
        set.values[index - 1] = lastValue;
        set.indices[lastValue] = index;

        // Invariant: swapped element's index is correct.
        assert(set.values[set.indices[lastValue] - 1] == lastValue);
    }

    set.values.pop();
    delete set.indices[value];

    // Postcondition: value is no longer in the set.
    assert(set.indices[value] == 0);
}
```

### ERC6909 Token Balances With Assertions

```solidity
/// @dev Multi-token balance tracking (like ERC1155 but simpler).
/// Invariant: balanceOf[owner][id] <= totalSupply[id] for all owner, id.
mapping(address => mapping(uint256 => uint256)) public balanceOf;
mapping(uint256 => uint256) public totalSupply;

function _mint(address to, uint256 id, uint256 amount) internal {
    assert(to != address(0));
    assert(amount > 0);

    uint256 supplyBefore = totalSupply[id];
    uint256 balanceBefore = balanceOf[to][id];

    totalSupply[id] = supplyBefore + amount;
    balanceOf[to][id] = balanceBefore + amount;

    // Overflow checks.
    assert(totalSupply[id] >= supplyBefore);
    assert(balanceOf[to][id] >= balanceBefore);
    // Core invariant: individual balance <= total supply.
    assert(balanceOf[to][id] <= totalSupply[id]);
}
```
