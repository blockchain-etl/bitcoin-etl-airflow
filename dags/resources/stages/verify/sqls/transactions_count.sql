select if((select sum(transaction_count) from `{{destination_dataset_project_id}}.{{dataset_name}}.blocks`) =
(select count(*) from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions`), 1,
cast((select 'Total number of transactions is not equal to sum of transaction_count in blocks table') as INT64))
