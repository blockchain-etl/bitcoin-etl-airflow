select if(
(select max(height) from `{{destination_dataset_project_id}}.{{dataset_name}}.blocks`) + 1 =
(select count(*) from `{{destination_dataset_project_id}}.{{dataset_name}}.blocks`), 1,
cast((select 'Total number of blocks except genesis is not equal to last block number {{ds}}') as INT64))
