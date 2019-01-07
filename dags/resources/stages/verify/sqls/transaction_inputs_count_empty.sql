select if((select count(*)
    from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions` as transactions
    where is_coinbase = false and input_count = 0
    ) = 0, 1,
cast((select 'Non-coinbase transaction has 0 inputs') as INT64))
