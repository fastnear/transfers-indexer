## Transfers Indexer

### Example .env

```
DATABASE_URL=http://localhost:8123
DATABASE_USER=default
DATABASE_PASSWORD=password
DATABASE_DATABASE=default
NUM_FETCHING_THREADS=8
CLICKHOUSE_SKIP_COMMIT=false
COMMIT_EVERY_BLOCK=false
CHAIN_ID=testnet
```

### Create clickhouse tables

For generic action view:

```clickhouse
CREATE TABLE transfers
(
    block_height                    UInt64 COMMENT 'Block height',
    block_timestamp                 DateTime64(9, 'UTC') COMMENT 'Block timestamp in nanoseconds using UTC',
    transaction_id                  Nullable(String) COMMENT 'Transaction hash. Sometimes our indexer is missing the transaction hash.',
    receipt_id                      String COMMENT 'Receipt hash',
    action_index                    Nullable(UInt16) COMMENT 'Index of the actions within the receipt. Empty for event based (where action index is unknown)',
    log_index                       Nullable(UInt16) COMMENT 'Index of the log within the receipt. Empty for action based transfers.',
    transfer_index                  UInt32 COMMENT 'The unique index of the transfer within the block',
    signer_id                       String COMMENT 'The account ID of the transaction signer',
    predecessor_id                  String COMMENT 'The account ID of the receipt predecessor',
    account_id                      String COMMENT 'The account ID of where the receipt is executed',
    sender_id                       Nullable(String) COMMENT 'The account ID of the sender, or empty for mints',
    receiver_id                     Nullable(String) COMMENT 'The account ID of the receiver, or empty for burns',
    asset_id                        String COMMENT 'The asset ID (e.g., "near" for NEAR transfers, or the token contract account ID for fungible token transfers)',
    asset_type                      LowCardinality(String) COMMENT 'The asset type: "Near" for native token transfers, "Ft" for fungible token transfers',
    amount                          UInt128 COMMENT 'The amount transferred in token units (e.g. yoctoNEAR)',
    method_name                     Nullable(String) COMMENT 'The method name that triggered the transfer (e.g., "ft_transfer", "ft_transfer_call", etc.)',
    transfer_type                   LowCardinality(String) COMMENT 'The type of transfer: NEAR native token or Fungible Token (FT)',
    human_amount                    Nullable(Float64) COMMENT 'The amount transferred after applying the token decimals, if available',
    usd_amount                      Nullable(Float64) COMMENT 'The USD value of the transfer at the time of the block, if available',
    sender_start_of_block_balance   Nullable(UInt128) COMMENT 'The sender account balance at the start of the block in token units',
    sender_end_of_block_balance     Nullable(UInt128) COMMENT 'The sender account balance at the end of the block in token units',
    receiver_start_of_block_balance Nullable(UInt128) COMMENT 'The receiver account balance at the start of the block in token units',
    receiver_end_of_block_balance   Nullable(UInt128) COMMENT 'The receiver account balance at the end of the block in token units',

    INDEX block_height_minmax_idx block_height TYPE minmax GRANULARITY 1,
    INDEX account_id_bloom_index account_id TYPE bloom_filter() GRANULARITY 1,
    INDEX asset_id_bloom_index asset_id TYPE bloom_filter() GRANULARITY 1,
    INDEX sender_id_bloom_index sender_id TYPE bloom_filter() GRANULARITY 1,
    INDEX receiver_id_bloom_index receiver_id TYPE bloom_filter() GRANULARITY 1
) ENGINE = ReplacingMergeTree
      PRIMARY KEY (block_timestamp)
      ORDER BY (block_timestamp, transfer_index);
```
