SELECT IF(
(SELECT MAX(height) FROM bitcoin_blockchain.blocks) + 1 =
(SELECT COUNT(*) FROM bitcoin_blockchain.blocks), 1,
CAST((SELECT 'Total number of blocks except genesis is not equal to last block number {{ds}}') AS INT64))
