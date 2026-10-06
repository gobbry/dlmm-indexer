-- Projections: read models maintained by the processor with value = value + delta.
CREATE TABLE pool_volume_1h(
    bucket timestamptz NOT NULL, -- hour start, UTC
    pool solana_address NOT NULL,
    swap_count bigint NOT NULL,
    volume_x numeric(39, 0) NOT NULL, -- base units of mint_x traded either way
    volume_y numeric(39, 0) NOT NULL,
    volume_usd numeric(38, 18) NOT NULL, -- sum over priced swaps only
    unpriced_swap_count bigint NOT NULL,
    PRIMARY KEY (pool, bucket)
);

CREATE INDEX pool_volume_1h_bucket ON pool_volume_1h(bucket, pool) INCLUDE (swap_count, unpriced_swap_count, volume_usd);

-- Same shape as pool_volume_1h; bucket is the UTC day start.
CREATE TABLE pool_volume_1d(
    LIKE pool_volume_1h INCLUDING ALL
);

CREATE TABLE pool_stats(
    pool solana_address PRIMARY KEY,
    swap_count bigint NOT NULL,
    volume_x numeric(39, 0) NOT NULL,
    volume_y numeric(39, 0) NOT NULL,
    volume_usd numeric(38, 18) NOT NULL,
    unpriced_swap_count bigint NOT NULL,
    first_swap_at timestamptz NOT NULL,
    last_swap_at timestamptz NOT NULL
);

CREATE TABLE projection(
    name text PRIMARY KEY,
    version integer NOT NULL,
    cursor_slot bigint NOT NULL DEFAULT 0,
    cursor_transaction_index smallint NOT NULL DEFAULT 0,
    cursor_swap_ordinal smallint NOT NULL DEFAULT 0,
    state projection_state NOT NULL DEFAULT 'live',
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO projection(name, version)
VALUES
    ('pool_volume_1h', 1),
('pool_volume_1d', 1),
('pool_stats', 1);

