merge {{params.dataset_name}}.transactions dest
using {{params.dataset_name_temp}}.{{params.source_table}} source
on false
when not matched and date(block_timestamp) = '{{ds}}' then
insert (
    `hash`,
    size,
    virtual_size,
    version,
    lock_time,
    block_hash,
    block_number,
    is_coinbase,
    block_timestamp,
    block_timestamp_month,
    input_count,
    output_count,
    input_value,
    output_value,
    inputs,
    outputs,
    fee
) values (
    `hash`,
    size,
    virtual_size,
    version,
    lock_time,
    block_hash,
    block_number,
    is_coinbase,
    block_timestamp,
    block_timestamp_month,
    input_count,
    output_count,
    input_value,
    output_value,
    inputs,
    outputs,
    fee
)
when not matched by source and date(block_timestamp) = '{{ds}}' then
delete
