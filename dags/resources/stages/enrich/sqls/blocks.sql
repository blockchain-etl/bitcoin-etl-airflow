SELECT
    `hash`,
    size,
    stripped_size,
    weight,
    number,
    version,
    merkle_root,
    TIMESTAMP_SECONDS(timestamp) as timestamp,
    nonce,
    bits,
    coinbase_param,
    transaction_count
FROM {{dataset_name_raw}}.blocks AS blocks
