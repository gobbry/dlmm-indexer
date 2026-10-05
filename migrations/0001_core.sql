CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Exclusion constraints over slot ranges.
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Domains: the invariants the Rust newtypes enforce, visible in the schema.
CREATE DOMAIN solana_address AS TEXT CHECK (VALUE ~ '^[1-9A-HJ-NP-Za-km-z]{32,44}$');

CREATE DOMAIN solana_signature AS TEXT CHECK (VALUE ~ '^[1-9A-HJ-NP-Za-km-z]{86,88}$');

CREATE DOMAIN u64 AS NUMERIC(20, 0) CHECK (VALUE >= 0
    AND VALUE <= 18446744073709551615);

-- An upper-case ticker, not an enum: onboarding a quote asset is a Rust variant, never a migration.
CREATE DOMAIN asset_symbol AS TEXT CHECK (VALUE ~ '^[A-Z0-9]{2,16}$');

CREATE TYPE swap_direction AS ENUM(
    'x_to_y',
    'y_to_x'
);

CREATE TYPE fee_side AS ENUM(
    'input',
    'output'
);

CREATE TYPE fee_token AS ENUM(
    'x',
    'y'
);

CREATE TYPE swap_source AS ENUM(
    'live_geyser',
    'live_rpc',
    'fill'
);

CREATE TYPE price_source AS ENUM(
    'binance',
    'peg',
    'carried_forward'
);

CREATE TYPE projection_state AS ENUM(
    'building',
    'live'
);

-- How a job's end_slot was set: a block (a hole's end, the archive window's top, an unmappable
-- slot), or the cut just below the archive's bottom, which may be a skipped slot only the
-- block above can prove. Recorded when the job is cut, so a later move of the archive's bottom
-- never changes how the job finishes.
CREATE TYPE job_end_kind AS ENUM(
    'block',
    'archive_lower_cut'
);

CREATE TABLE pool(
    address solana_address PRIMARY KEY,
    mint_x solana_address NOT NULL,
    mint_y solana_address NOT NULL,
    first_seen_slot bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE token(
    mint solana_address PRIMARY KEY,
    decimals smallint, -- null until fetched
    fetched_at timestamptz
);

CREATE TABLE swap(
    block_time timestamptz NOT NULL,
    slot bigint NOT NULL,
    transaction_index smallint NOT NULL, -- position in block; log order
    signature solana_signature NOT NULL,
    swap_ordinal smallint NOT NULL,
    pool solana_address NOT NULL REFERENCES pool(address),
    user_address solana_address NOT NULL,
    direction swap_direction NOT NULL,
    mint_in solana_address NOT NULL,
    mint_out solana_address NOT NULL,
    amount_in u64 NOT NULL,
    amount_out u64 NOT NULL,
    fee u64 NOT NULL, -- Swap.fee, base units of the fee token
    protocol_fee u64 NOT NULL,
    host_fee u64 NOT NULL,
    fee_rate_1e9 u64 NOT NULL, -- the IDL's fee_bps: a rate x 1e9
    start_bin_id integer NOT NULL,
    end_bin_id integer NOT NULL,
    mm_fee u64, -- Swap2Evt (0.12.0 layout) or null
    limit_order_fee u64,
    amount_left u64,
    fee_side fee_side,
    fee_token fee_token,
    swap2_event_payload bytea, -- raw Swap2Evt for layouts not decoded
    quote_asset_symbol asset_symbol, -- null = unpriceable pool
    quote_amount u64, -- base units of the quote leg
    price_ts timestamptz, -- ts of the price row that priced it
    volume_usd numeric(38, 18), -- null = unpriced (yet)
    source swap_source NOT NULL,
    fill_job_id bigint,
    CHECK ((source = 'fill') =(fill_job_id IS NOT NULL)),
    UNIQUE (signature, swap_ordinal, block_time)
)
WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_time',
    tsdb.chunk_interval = '1 day',
    tsdb.segmentby = 'pool',
    tsdb.orderby = 'block_time DESC'
);

CREATE INDEX swap_pool_time ON swap(pool, block_time DESC);

CREATE UNIQUE INDEX swap_log_order ON swap(slot, transaction_index, swap_ordinal, block_time);

CREATE INDEX swap_unpriced ON swap(block_time)
WHERE
    volume_usd IS NULL AND quote_asset_symbol IS NOT NULL;

CREATE TABLE decode_failure(
    block_time timestamptz NOT NULL,
    slot bigint NOT NULL,
    signature solana_signature NOT NULL,
    reason text NOT NULL, -- 'unmappable: …' for mapping failures
    PRIMARY KEY (signature, reason)
);

CREATE TABLE price(
    asset_symbol asset_symbol NOT NULL,
    ts timestamptz NOT NULL,
    source price_source NOT NULL,
    close_usd numeric(18, 8) NOT NULL,
    PRIMARY KEY (asset_symbol, ts, source)
);

-- The slot ranges fully indexed: every block write covers (parent_slot, slot]. The top range's
-- end is the cursor.
CREATE TABLE slot_coverage(
    start_slot bigint NOT NULL,
    end_slot bigint NOT NULL, -- inclusive, always a block's slot
    end_block_time timestamptz NOT NULL, -- block_time of the block at end_slot
    CHECK (start_slot <= end_slot),
    EXCLUDE USING gist(int8range(start_slot, end_slot, '[]'
) WITH &&)
);

CREATE TABLE slot_range_job(
    id bigserial PRIMARY KEY,
    start_slot bigint NOT NULL,
    end_slot bigint NOT NULL, -- inclusive
    next_slot bigint NOT NULL, -- first slot not yet stored
    end_kind job_end_kind NOT NULL DEFAULT 'block',
    blocked_reason text, -- set when the node lacks a slot or a block is unmappable
    completed_at timestamptz, -- set by the reconciler once coverage contains the range
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (start_slot <= end_slot),
    CHECK (next_slot >= start_slot),
    EXCLUDE USING gist(int8range(start_slot, end_slot, '[]'
) WITH &&)
);

CREATE INDEX slot_range_job_open ON slot_range_job(end_slot DESC)
WHERE
    completed_at IS NULL AND blocked_reason IS NULL AND next_slot <= end_slot;
