merge {{dataset_name}}.blocks dest
using {{dataset_name_temp}}.{{source_table}} source
on false
when not matched and date(timestamp) = '{{ds}}' then
insert (
    `hash`,
    size,
    stripped_size,
    weight,
    number,
    version,
    merkle_root,
    timestamp,
    timestamp_month,
    nonce,
    bits,
    coinbase_param,
    transaction_count
) values (
    `hash`,
    size,
    stripped_size,
    weight,
    number,
    version,
    merkle_root,
    timestamp,
    timestamp_month,
    nonce,
    bits,
    coinbase_param,
    transaction_count
)
when not matched by source and date(timestamp) = '{{ds}}' then
delete
