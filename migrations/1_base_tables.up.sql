CREATE TABLE blocks (
    id SERIAL PRIMARY KEY,
    block_hash TEXT NOT NULL,
    parent_hash TEXT NOT NULL,
    block_time BIGINT NOT NULL,
    tx_count BIGINT NOT NULL,
    tx_plus_prev_count BIGINT NULL,
    ax_count BIGINT NOT NULL,
    ax_plus_prev_count BIGINT NULL,

    INDEX blocks_tx_count_idx (tx_count),
    INDEX blocks_tx_plus_prev_count_idx (tx_plus_prev_count),
    INDEX blocks_ax_count_idx (ax_count),
    INDEX blocks_ax_plus_prev_count_idx (ax_plus_prev_count)
);