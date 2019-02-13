select if((select count(*)
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` as transactions
    where output_count != array_length(outputs)
        and date(block_timestamp) <= '{{ds}}'
    ) = 0, 1,
cast((select 'Output counts dont match') as INT64))
