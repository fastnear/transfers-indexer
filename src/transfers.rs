use crate::*;
use std::collections::HashSet;

use crate::block_indexer::BlockIndexer;
use crate::rpc::RpcConfig;
use crate::types::*;
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use fastnear_primitives::near_primitives::types::BlockHeight;
use reqwest::Client;

pub struct TransfersIndexer {
    pub commit_every_block: bool,
    pub rows: Vec<TransferRow>,
    pub commit_handlers: Vec<tokio::task::JoinHandle<Result<(), clickhouse::error::Error>>>,
    pub transfer_types: Option<HashSet<TransferType>>,
    pub task_cache: Option<TaskCache>,
    pub rpc_config: RpcConfig,
    pub client: Client,
    pub price_history: Option<PriceHistorySingleton>,
    pub previous_epoch_id: Option<CryptoHash>,
}

impl TransfersIndexer {
    pub fn new(transfer_types: Option<&[TransferType]>) -> Self {
        let commit_every_block = std::env::var("COMMIT_EVERY_BLOCK")
            .map(|v| v == "true")
            .unwrap_or(false);
        let rpc_config = RpcConfig::from_env();
        Self {
            commit_every_block,
            rows: vec![],
            commit_handlers: vec![],
            transfer_types: transfer_types.map(|types| types.iter().copied().collect()),
            task_cache: None,
            rpc_config,
            client: Client::new(),
            price_history: None,
            previous_epoch_id: None,
        }
    }

    pub async fn maybe_commit(
        &mut self,
        db: &ClickDB,
        block_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let is_round_block = block_height % SAVE_STEP == 0;
        if is_round_block {
            tracing::log::info!(
                target: CLICKHOUSE_TARGET,
                "#{}: Having {} rows",
                block_height,
                self.rows.len(),
            );
        }
        if self.rows.len() >= db.min_batch || is_round_block || self.commit_every_block {
            self.commit(db).await?;
        }

        Ok(())
    }

    pub async fn commit(&mut self, db: &ClickDB) -> anyhow::Result<()> {
        let mut rows = vec![];
        std::mem::swap(&mut rows, &mut self.rows);
        while self.commit_handlers.len() >= MAX_COMMIT_HANDLERS {
            self.commit_handlers.remove(0).await??;
        }
        let db = db.clone();
        let handler = tokio::spawn(async move {
            if !rows.is_empty() {
                insert_rows_with_retry(&db.client, &rows, "account_transfers").await?;
            }
            tracing::log::info!(
                target: CLICKHOUSE_TARGET,
                "Committed {} rows",
                rows.len(),
            );
            Ok::<(), clickhouse::error::Error>(())
        });
        self.commit_handlers.push(handler);

        Ok(())
    }

    pub async fn process_block(
        &mut self,
        db: &ClickDB,
        mut block: BlockWithTxHashes,
    ) -> anyhow::Result<()> {
        block.shards.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));

        let block_height = block.block.header.height;
        let current_epoch_id = block.block.header.epoch_id;
        let is_epoch_boundary = self
            .previous_epoch_id
            .map_or(false, |prev| prev != current_epoch_id);
        self.previous_epoch_id = Some(current_epoch_id);

        let mut block_indexer = BlockIndexer::new(self.task_cache.take());
        block_indexer.process_block(block, &self.transfer_types, is_epoch_boundary)?;

        // Execute tasks and fill pending rows
        let (rows, task_cache) = block_indexer
            .execute_tasks(&self.client, &self.rpc_config, &self.price_history)
            .await?;
        self.rows.extend(rows);
        self.task_cache = Some(task_cache);

        let is_round_block = block_height % SAVE_STEP == 0;
        if is_round_block {
            tracing::log::info!(target: CLICKHOUSE_TARGET, "#{}: Having {} rows", block_height, self.rows.len());
        }

        self.maybe_commit(db, block_height).await?;
        Ok(())
    }

    pub async fn last_block_in_range(
        &self,
        db: &ClickDB,
        start_block: BlockHeight,
        end_block: BlockHeight,
    ) -> BlockHeight {
        let res = db
            .max_in_range("block_height", "account_transfers", start_block, end_block)
            .await
            .unwrap_or(0);
        if res == 0 {
            start_block.saturating_sub(1)
        } else {
            res
        }
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        while let Some(handler) = self.commit_handlers.pop() {
            handler.await??;
        }
        Ok(())
    }
}
