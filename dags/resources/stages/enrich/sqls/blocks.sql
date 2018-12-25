select
    `hash`,
    size,
    stripped_size,
    weight,
    number,
    version,
    merkle_root,
    timestamp_seconds(timestamp) as timestamp,
    date_trunc(date(timestamp_seconds(timestamp)), MONTH) as timestamp_month,
    nonce,
    bits,
    coinbase_param,
    transaction_count
from {{dataset_name_raw}}.blocks as blocks
