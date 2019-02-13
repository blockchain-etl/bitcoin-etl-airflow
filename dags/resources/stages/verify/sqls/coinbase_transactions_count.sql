select if((
select count(*)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`
where date(timestamp) <= '{{ds}}') =
(select count(*)
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions`
    where is_coinbase = true and date(block_timestamp) <= '{{ds}}'), 1,
cast((select 'Total number of transactions with 0 inputs is not equal to number of blocks') as INT64))
