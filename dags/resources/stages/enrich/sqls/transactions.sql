with flat_inputs as (
    select transactions.`hash`, transactions.block_timestamp, inputs.*
    from {{dataset_name_raw}}.transactions as transactions,
    unnest(inputs) as inputs
),
flat_outputs as (
    select transactions.`hash`, transactions.block_timestamp, outputs.*
    from {{dataset_name_raw}}.transactions as transactions,
    unnest(outputs) as outputs
),
enriched_flat_inputs as (
    select
        flat_inputs.index,
        flat_inputs.`hash`,
        flat_inputs.block_timestamp,
        flat_outputs.required_signatures,
        flat_outputs.type,
        flat_outputs.addresses,
        flat_outputs.value
    from flat_inputs
    left join flat_outputs on flat_inputs.spent_transaction_hash = flat_outputs.`hash`
        and flat_inputs.spent_output_index = flat_outputs.index
),
grouped_enriched_inputs as (
    select `hash`, block_timestamp, array_agg(struct(index, required_signatures, type, addresses, value)) as inputs
    from enriched_flat_inputs
    group by `hash`, block_timestamp
)
select
    transactions.`hash`,
    transactions.size,
    transactions.virtual_size,
    transactions.version,
    transactions.lock_time,
    transactions.block_hash,
    transactions.block_number,
    timestamp_seconds(transactions.block_timestamp) as block_timestamp,
    transactions.input_count,
    transactions.output_count,
    array(
      select as struct inputs.index, inputs.spent_transaction_hash, inputs.spent_output_index,
          inputs.script_asm, inputs.script_hex,
          enriched_inputs.required_signatures, enriched_inputs.type, enriched_inputs.addresses, enriched_inputs.value
      from unnest(grouped_enriched_inputs.inputs) as enriched_inputs
      join unnest(transactions.inputs) as inputs on inputs.index = enriched_inputs.index
      order by inputs.index
    ) as inputs,
    transactions.outputs
from {{dataset_name_raw}}.transactions as transactions
left join grouped_enriched_inputs on grouped_enriched_inputs.`hash` = transactions.`hash`
    and grouped_enriched_inputs.block_timestamp = transactions.block_timestamp
