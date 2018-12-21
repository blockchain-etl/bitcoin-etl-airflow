select
    `hash`,
    size,
    stripped_size,
    weight,
    number,
    version,
    merkle_root,
    timestamp_seconds(timestamp) as timestamp,
    nonce,
    bits,
    coinbase_param,
    transaction_count
from {{dataset_name_raw}}.blocks as blocks
