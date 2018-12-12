SELECT IF((SELECT sum(transaction_count) FROM bitcoin_blockchain.blocks) =
(SELECT COUNT(*) FROM bitcoin_blockchain.transactions), 1,
CAST((SELECT 'Total number of transactions is not equal to sum of transaction_count in blocks table') AS INT64))
