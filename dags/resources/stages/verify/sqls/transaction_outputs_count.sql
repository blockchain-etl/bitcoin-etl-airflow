select if((select count(*)
    from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions` as transactions
    where output_count != array_length(outputs)
    ) = 0, 1,
cast((select 'Output counts dont match') as INT64))
