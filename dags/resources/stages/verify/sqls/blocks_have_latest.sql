select if(
(select count(*) from `{{destination_dataset_project_id}}.{{dataset_name}}.blocks` where date(timestamp) = '{{ds}}') > 0, 1,
cast((select 'There are no blocks on {{ds}}') as INT64))
