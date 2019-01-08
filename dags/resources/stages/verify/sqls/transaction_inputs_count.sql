select if((select count(*)
    from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions` as transactions
    where input_count != array_length(inputs)
    ) = 0, 1,
cast((select 'Input counts dont match') as INT64))
