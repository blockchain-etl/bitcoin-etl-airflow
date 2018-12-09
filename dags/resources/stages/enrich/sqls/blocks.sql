SELECT
    hash,
    size,
    stripped_size,
    weight,
    height,
    version,
    merkle_root,
    TIMESTAMP_SECONDS(time) as time,
    median_time,
    nonce,
    bits,
    transaction_count
FROM {{DATASET_NAME_RAW}}.blocks AS blocks
