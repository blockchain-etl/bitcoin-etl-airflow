select if(
(
select timestamp_diff(
  current_timestamp(),
  (select max(timestamp)
  from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks` as blocks
  where timestamp_month >= date_trunc('{{ds}}', MONTH)),
  MINUTE)
) < {{params.max_lag_in_minutes}}, 1,
cast((select 'Blocks are lagging by more than {{params.max_lag_in_minutes}} minutes') as INT64))
