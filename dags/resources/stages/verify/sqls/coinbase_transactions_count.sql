select if((select count(*) from `{{destination_dataset_project_id}}.{{dataset_name}}.blocks`) =
(select count(*)
    from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions`
    where is_coinbase = true), 1,
cast((select 'Total number of transactions with 0 inputs is not equal to number of blocks') as INT64))
