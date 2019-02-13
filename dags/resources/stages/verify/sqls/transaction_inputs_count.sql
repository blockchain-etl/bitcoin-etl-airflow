select if((select count(*)
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` as transactions
    where input_count != array_length(inputs)
        and date(block_timestamp) <= '{{ds}}'
    ) = 0, 1,
cast((select 'Input counts dont match') as INT64))
