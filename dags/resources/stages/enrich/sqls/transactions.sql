with flat_inputs as (
    select transactions.txid, transactions.block_time, inputs.*
    from bitcoin_blockchain_raw.transactions_raw as transactions,
    unnest(inputs) as inputs
),
flat_outputs as (
    select transactions.txid, transactions.block_time, outputs.*
    from bitcoin_blockchain_raw.transactions_raw as transactions,
    unnest(outputs) as outputs
),
enriched_flat_inputs as (
    select
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
    select txid, block_time, array_agg(struct(required_signatures, type, addresses, value)) as inputs
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
    TIMESTAMP_SECONDS(transactions.block_time) as block_time,
    transactions.block_median_time,
    grouped_enriched_inputs.inputs,
    transactions.outputs
FROM bitcoin_blockchain_raw.transactions_raw AS transactions
join grouped_enriched_inputs on grouped_enriched_inputs.txid = transactions.txid
    and grouped_enriched_inputs.block_time = transactions.block_time
