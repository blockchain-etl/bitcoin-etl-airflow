SELECT
  transactions.hash
, outputs.index
, outputs.script_asm
, outputs.script_hex
, outputs.sequence
, outputs.required_signatures
, outputs.type
, outputs.addresses
, outputs.value
FROM `{{destination_dataset_project_id}}.{{dataset_name}}.transactions` as transactions
CROSS JOIN UNNEST(transactions.outputs) as outputs
