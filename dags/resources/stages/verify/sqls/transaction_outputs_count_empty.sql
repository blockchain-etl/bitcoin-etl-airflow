select if((select count(*)
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` as transactions
    where output_count = 0
        and date(block_timestamp) <= '{{ds}}'
    ) = 0, 1,
cast((select 'Transaction has 0 outputs') as INT64))
