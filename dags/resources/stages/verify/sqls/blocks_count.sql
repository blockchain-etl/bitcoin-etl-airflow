SELECT IF(
(SELECT MAX(height) FROM `{{destination_dataset_project_id}}.{{dataset_name}}.blocks`) + 1 =
(SELECT COUNT(*) FROM `{{destination_dataset_project_id}}.{{dataset_name}}.blocks`), 1,
CAST((SELECT 'Total number of blocks except genesis is not equal to last block number {{ds}}') AS INT64))
