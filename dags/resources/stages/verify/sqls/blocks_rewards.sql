select if(
(with transaction_fees_per_block as (
    select block_number, sum(fee) as fee
    from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions`
    where is_coinbase = false
    group by block_number
), block_rewards as (
    select transactions.block_number, (transactions.output_value - transaction_fees_per_block.fee) as block_reward
    from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions` as transactions
    join transaction_fees_per_block on transaction_fees_per_block.block_number = transactions.block_number
    where transactions.is_coinbase = true
), erroneous_blocks as (
    select block_number
    from block_rewards
    where block_reward % (5000000000 / (2 ** 9)) != 0
)
select count(*)
from erroneous_blocks
) = 0, 1,
cast((select 'Total number of blocks except genesis is not equal to last block number {{ds}}') as INT64))
