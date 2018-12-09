with flat_inputs as (
    select transactions.txid, inputs.*
    from bitcoin_blockchain_raw.transactions,
    unnest(inputs) as inputs
),
flat_outputs as (
    select transactions.txid, outputs.*
    from bitcoin_blockchain_raw.transactions,
    unnest(outputs) as outputs
),
enriched_flat_inputs as (
    select
        flat_inputs.txid,
        flat_inputs.spent_txid,
        flat_inputs.spent_output_index,
        flat_inputs.script_asm,
        flat_inputs.script_hex,
        flat_inputs.coinbase_param,
        flat_inputs.sequence,
        flat_outputs.addresses,
        flat_outputs.value
    from flat_inputs
    left join flat_outputs on flat_inputs.spent_txid = flat_outputs.txid
        and flat_inputs.spent_output_index = flat_outputs.index
),
grouped_enriched_inputs as (
    select txid, array_agg(struct(spent_txid, spent_output_index, script_asm, script_hex, coinbase_param, sequence, addresses, value)) as inputs
    from enriched_flat_inputs
    group by txid
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
FROM bitcoin_blockchain_raw.transactions AS transactions
join grouped_enriched_inputs on grouped_enriched_inputs. txid = transactions.txid
