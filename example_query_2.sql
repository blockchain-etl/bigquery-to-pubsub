SELECT 
  CASE addr_hot.address WHEN tx.from_address THEN addr_hot.exchange ELSE addr_usr.exchange END AS exchange,
  CASE addr_hot.address WHEN tx.from_address THEN 'out' ELSE 'in' END AS io,
  tx.*
FROM
  `bigquery-public-data.crypto_ethereum.transactions` AS tx,
  `exchange-flow-demo.exchange_wallet_addresses.exchange_hotwallet_addresses` AS addr_hot,
  `exchange-flow-demo.exchange_wallet_addresses.exchange_hotwallet_addresses` AS addr_usr
WHERE TRUE
  AND block_timestamp >= '2019-10-23 00:00:00' 
  AND block_timestamp < '2019-10-24 00:00:00' 
  AND (tx.from_address = addr_hot.address OR tx.to_address = addr_usr.address)
