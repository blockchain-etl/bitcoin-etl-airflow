select if((select sum(output_count) from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions`) =
(select count(*)
    from `{{destination_dataset_project_id}}.{{dataset_name}}.transactions` as transactions,
    transactions.outputs as outputs), 1,
cast((select 'Total number of outputs in transactions is not equal to sum of output_count in transactions table') as INT64))
