with flat_inputs as (
    select transactions.txid, transactions.block_time, inputs.*
    from {{dataset_name_raw}}.transactions as transactions,
    unnest(inputs) as inputs
),
flat_outputs as (
    select transactions.txid, transactions.block_time, outputs.*
    from {{dataset_name_raw}}.transactions as transactions,
    unnest(outputs) as outputs
),
enriched_flat_inputs as (
    select
        flat_inputs.index,
        flat_inputs.txid,
        flat_inputs.block_time,
        flat_outputs.required_signatures,
        flat_outputs.type,
        flat_outputs.addresses,
        flat_outputs.value
    from flat_inputs
    left join flat_outputs on flat_inputs.spent_txid = flat_outputs.txid
        and flat_inputs.spent_output_index = flat_outputs.index
),
grouped_enriched_inputs as (
    select txid, block_time, array_agg(struct(index, required_signatures, type, addresses, value)) as inputs
    from enriched_flat_inputs
    group by txid, block_time
)
SELECT
    transactions.txid,
    transactions.`hash`,
    transactions.size,
    transactions.virtual_size,
    transactions.version,
    transactions.lock_time,
    transactions.block_hash,
    transactions.block_height,
    TIMESTAMP_SECONDS(transactions.block_time) as block_time,
    array(
      select as struct inputs.index, inputs.script_asm, inputs.coinbase_param,
          enriched_inputs.required_signatures, enriched_inputs.type, enriched_inputs.addresses, enriched_inputs.value
      from unnest(grouped_enriched_inputs.inputs) as enriched_inputs
      join unnest(transactions.inputs) as inputs on inputs.index = enriched_inputs.index
      order by inputs.index
    ) as inputs,
    transactions.outputs
FROM {{dataset_name_raw}}.transactions AS transactions
join grouped_enriched_inputs on grouped_enriched_inputs.txid = transactions.txid
    and grouped_enriched_inputs.block_time = transactions.block_time
