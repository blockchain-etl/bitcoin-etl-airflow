select if((select count(*)
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` as transactions
    where is_coinbase = false
        and input_count = 0
        and date(block_timestamp) <= '{{ds}}'
    ) = 0, 1,
cast((select 'Non-coinbase transaction has 0 inputs') as INT64))
