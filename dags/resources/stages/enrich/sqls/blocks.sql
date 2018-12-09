SELECT
    `hash`,
    size,
    stripped_size,
    weight,
    height,
    version,
    merkle_root,
    TIMESTAMP_SECONDS(time) as time,
    TIMESTAMP_SECONDS(median_time) as median_time,
    nonce,
    bits,
    transaction_count
FROM bitcoin_blockchain_raw.blocks AS blocks
