SELECT IF(
(SELECT COUNT(*) FROM `{{destination_dataset_project_id}}.{{dataset_name}}.blocks` WHERE DATE(timestamp) = '{{ds}}') > 0, 1,
CAST((SELECT 'There are no blocks on {{ds}}') AS INT64))
