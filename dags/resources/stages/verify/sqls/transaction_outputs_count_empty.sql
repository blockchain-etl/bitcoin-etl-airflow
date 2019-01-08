select if((select count(*)
    from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions` as transactions
    where output_count = 0
    ) = 0, 1,
cast((select 'Transaction has 0 outputs') as INT64))
