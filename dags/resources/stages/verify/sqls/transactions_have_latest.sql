select if(
(select count(*) from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions` where date(block_timestamp) = '{{ds}}') > 0, 1,
cast((select 'There are no transactions on {{ds}}') as INT64))
