select if((select count(*)
    from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions`
    where fee < 0) = 0, 1,
cast((select 'There are transactions with negative fees') as INT64))
