SELECT
  transactions.hash
, inpts.spent_transaction_hash
, inpts.spent_output_index
, inpts.script_asm
, inpts.script_hex
, inpts.sequence
, inpts.required_signatures
, inpts.type
, inpts.addresses
, inpts.value
FROM `{{destination_dataset_project_id}}.{{dataset_name}}.transactions` as transactions
CROSS JOIN UNNEST(transactions.inputs) as inpts
