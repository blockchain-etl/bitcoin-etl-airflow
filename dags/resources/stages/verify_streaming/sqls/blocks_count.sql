select if(
(
  select count(*) - (max(number) - min(number))
  from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks` as blocks
  where date(timestamp) = '{{ds}}'
  and timestamp_month >= date_trunc('{{ds}}', MONTH)
) between 0 and 2, 1,
cast((select 'There are missing blocks or more than 2 duplicate block numbers') as INT64))
