select if(abs(
  coalesce((
      select sum(transaction_count)
      from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks` as blocks
      where date(timestamp) = '{{ds}}'
          and timestamp_month >= date_trunc('{{ds}}', MONTH)
  ),0 ) -
  coalesce((
      select count(*)
      from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` as transactions
      where date(block_timestamp) = '{{ds}}'
          and block_timestamp_month >= date_trunc('{{ds}}', MONTH)
  ), 0)) <
  coalesce((
      select avg(transaction_count)
      from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks` as blocks
      where date(timestamp) = '{{ds}}'
          and timestamp_month >= date_trunc('{{ds}}', MONTH)
  ), 100) * 2, 1,
cast((select 'The difference between number of transactions and sum of transaction_count in blocks table is greater than average transaction number in a block by more than 2 times') as INT64))
