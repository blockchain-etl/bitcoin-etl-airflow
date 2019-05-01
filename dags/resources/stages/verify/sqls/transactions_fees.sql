select if((select count(*)
    from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions`
    where (fee < 0 or fee is null)
        and date(block_timestamp) <= '{{ds}}') = 0, 1,
cast((select 'There are transactions with negative fees') as INT64))
