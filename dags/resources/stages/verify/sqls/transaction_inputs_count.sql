select if((select sum(input_count) from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions`) =
(select count(*)
    from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions` as transactions,
    transactions.inputs as inputs), 1,
cast((select 'Total number of inputs in transactions is not equal to sum of input_count in transactions table') as INT64))
