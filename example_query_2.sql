SELECT
  tx.block_timestamp,
  tx.`hash`,
  CAST(tx.value / POW(10,18) AS STRING) AS value,
  CASE addr_hot.address WHEN tx.from_address THEN addr_hot.exchange ELSE addr_usr.exchange END AS source,
  CASE addr_hot.address WHEN tx.to_address THEN addr_hot.exchange ELSE addr_usr.exchange END AS target
FROM
  `bigquery-public-data.crypto_ethereum.transactions` AS tx,
  `exchange-flow-demo.exchange_wallet_addresses.exchange_hotwallet_addresses` AS addr_hot,
  `exchange-flow-demo.exchange_wallet_addresses.exchange_hotwallet_addresses` AS addr_usr
WHERE TRUE
  AND block_timestamp >= '2019-10-23 00:00:00'
  AND block_timestamp < '2019-10-24 00:00:00'
  AND (tx.from_address = addr_hot.address OR tx.to_address = addr_usr.address)
  AND value != 0
GROUP BY source, target, block_timestamp, `hash`, value
HAVING source != target
