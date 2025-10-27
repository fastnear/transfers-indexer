use crate::*;
use std::collections::HashSet;

use crate::block_indexer::BlockIndexer;
use crate::rpc::RpcConfig;
use crate::types::*;
use fastnear_primitives::near_primitives::types::BlockHeight;

pub struct TransfersIndexer {
    pub commit_every_block: bool,
    pub rows: Vec<TransferRow>,
    pub commit_handlers: Vec<tokio::task::JoinHandle<Result<(), clickhouse::error::Error>>>,
    pub transfer_types: Option<HashSet<TransferType>>,
    pub task_cache: Option<TaskCache>,
    pub rpc_config: RpcConfig,
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
                insert_rows_with_retry(&db.client, &rows, "transfers").await?;
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

        let mut block_indexer = BlockIndexer::new(self.task_cache.take());
        block_indexer.process_block(block, &self.transfer_types)?;

        // Execute tasks and fill pending rows
        let (rows, task_cache) = block_indexer.execute_tasks(&self.rpc_config).await?;
        self.rows.extend(rows);
        self.task_cache = Some(task_cache);

        let is_round_block = block_height % SAVE_STEP == 0;
        if is_round_block {
            tracing::log::info!(target: CLICKHOUSE_TARGET, "#{}: Having {} rows", block_height, self.rows.len());
        }

        self.maybe_commit(db, block_height).await?;
        Ok(())
    }

    pub async fn last_block_height(&self, db: &ClickDB) -> BlockHeight {
        db.max("block_height", "transfers").await.unwrap_or(0)
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        while let Some(handler) = self.commit_handlers.pop() {
            handler.await??;
        }
        Ok(())
    }
}
