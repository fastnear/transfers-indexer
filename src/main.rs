mod click;
mod common;

mod types;

use crate::click::*;
use std::sync::Arc;

use crate::types::*;
use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::near_indexer_primitives::types::BlockHeight;
use fastnear_primitives::types::ChainId;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

const PROJECT_ID: &str = "transfer-indexer";

#[tokio::main]
async fn main() {
    #[allow(deprecated)]
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let is_running = Arc::new(AtomicBool::new(true));
    let ctrl_c_running = is_running.clone();
    tokio::spawn(async move {
        let mut signals = signal_hook::iterator::Signals::new(&[
            signal_hook::consts::SIGTERM,
            signal_hook::consts::SIGINT,
        ])
        .unwrap();
        for sig in signals.forever() {
            match sig {
                signal_hook::consts::SIGTERM | signal_hook::consts::SIGINT => {
                    println!("Received signal {}, shutting down...", sig);
                    ctrl_c_running.store(false, Ordering::SeqCst);
                    break;
                }
                _ => unreachable!(),
            }
        }
    });

    common::setup_tracing(
        "clickhouse=info,transfer-indexer=info,neardata-fetcher=info,rpc=info,pricing-fetcher=info",
    );

    tracing::log::info!(target: PROJECT_ID, "Starting Transfer Indexer");

    let legacy_db = ClickDB::new(10000, "LEGACY_");
    legacy_db
        .verify_connection()
        .await
        .expect("Failed to connect to Legacy Clickhouse");

    let new_db = ClickDB::new(100000, "NEW_");
    new_db
        .verify_connection()
        .await
        .expect("Failed to connect to New Clickhouse");

    let client = reqwest::Client::new();
    let chain_id = ChainId::try_from(std::env::var("CHAIN_ID").expect("CHAIN_ID is not set"))
        .expect("Invalid chain id");

    let first_block_height = fetcher::fetch_first_block(&client, chain_id)
        .await
        .expect("First block doesn't exists")
        .block
        .header
        .height;

    tracing::log::info!(target: PROJECT_ID, "First block: {}", first_block_height);

    let args: Vec<String> = std::env::args().collect();

    let start_block_height: Option<BlockHeight> = args
        .get(1)
        .map(|v| v.parse().expect("Failed to parse backfill block height"));

    let end_block_height: Option<BlockHeight> = args
        .get(2)
        .map(|v| v.parse().expect("Failed to parse end block height"));

    let mut transfers_indexer = TransfersIndexer::new("account_transfers");

    let db_last_block_height = transfers_indexer
        .last_block_in_range(
            &new_db,
            start_block_height.unwrap_or(0),
            end_block_height.unwrap_or(10u64.pow(15)),
        )
        .await;
    tracing::log::info!(target: PROJECT_ID, "Last block height in range: {}", db_last_block_height);
    let start_block_height = (db_last_block_height + 1).max(first_block_height);

    if let Some(end_block_height) = end_block_height {
        if start_block_height >= end_block_height {
            tracing::log::info!(target: PROJECT_ID, "Nothing to do. The range is full.");
            return;
        }
    }

    // Legacy transfer rows
    let (sender, mut receiver) = mpsc::channel(100000);

    // Spawn fetcher task from legacy DB
    let fetcher_is_running = is_running.clone();
    tokio::spawn(async move {
        fetch_transfer_rows_from_legacy_db(
            &legacy_db,
            start_block_height,
            end_block_height,
            sender,
            fetcher_is_running,
        )
        .await
        .unwrap();
    });

    while let Some(row) = receiver.recv().await {
        if is_running.load(Ordering::SeqCst) {
            transfers_indexer
                .process_transfer_row(&new_db, row)
                .await
                .unwrap();
        }
    }

    if !is_running.load(Ordering::SeqCst) {
        // Truncate rows of the last block to avoid partial data
        let last_block_height = transfers_indexer.rows.last().map(|r| r.block_height);
        if let Some(last_block_height) = last_block_height {
            transfers_indexer
                .rows
                .retain(|r| r.block_height != last_block_height);
        }
    }
    transfers_indexer.commit(&new_db).await.unwrap();
    transfers_indexer.flush().await.unwrap();

    tracing::log::info!(target: PROJECT_ID, "Gracefully shut down");
}

pub struct TransfersIndexer {
    pub commit_every_block: bool,
    pub rows: Vec<AccountTransferRow>,
    pub commit_handlers: Vec<tokio::task::JoinHandle<Result<(), clickhouse::error::Error>>>,
    pub table: String,
    pub last_block_height: Option<BlockHeight>,
}

impl TransfersIndexer {
    pub fn new(table: &str) -> Self {
        let commit_every_block = std::env::var("COMMIT_EVERY_BLOCK")
            .map(|v| v == "true")
            .unwrap_or(false);
        Self {
            commit_every_block,
            rows: vec![],
            commit_handlers: vec![],
            table: table.to_string(),
            last_block_height: None,
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
            tracing::log::info!(
                target: CLICKHOUSE_TARGET,
                "#{}: Committing {} rows",
                block_height,
                self.rows.len(),
            );
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
        let table = self.table.clone();
        let handler = tokio::spawn(async move {
            if !rows.is_empty() {
                insert_rows_with_retry(&db.client, &rows, &table).await?;
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

    pub async fn process_transfer_row(
        &mut self,
        db: &ClickDB,
        row: TransferRow,
    ) -> anyhow::Result<()> {
        let block_height = row.block_height;
        if let Some(last_block_height) = self.last_block_height {
            if block_height < last_block_height {
                return Ok(());
            }
            if block_height > last_block_height {
                self.maybe_commit(db, block_height).await?;
            }
        }
        self.last_block_height = Some(block_height);

        let mut account_row = AccountTransferRow {
            block_height: row.block_height,
            block_timestamp: row.block_timestamp,
            transaction_id: row.transaction_id,
            receipt_id: row.receipt_id,
            action_index: row.action_index,
            log_index: row.log_index,
            transfer_index: row.transfer_index,
            signer_id: row.signer_id,
            predecessor_id: row.predecessor_id,
            receipt_account_id: row.account_id,
            account_id: "".to_string(),
            other_account_id: None,
            asset_id: row.asset_id,
            asset_type: row.asset_type,
            amount: row.amount.min(i128::MAX as u128) as i128,
            method_name: row.method_name,
            transfer_type: row.transfer_type,
            human_amount: row.human_amount,
            usd_amount: row.usd_amount,
            start_of_block_balance: row.receiver_start_of_block_balance,
            end_of_block_balance: row.receiver_end_of_block_balance,
        };

        if row.sender_id != row.receiver_id {
            if let Some(sender_id) = &row.sender_id {
                let mut account_row = account_row.clone();
                account_row.amount = -account_row.amount;
                account_row.account_id = sender_id.to_string();
                account_row.other_account_id = row.receiver_id.as_ref().map(|id| id.to_string());
                account_row.human_amount = row.human_amount.map(|v| -v);
                account_row.usd_amount = row.usd_amount.map(|v| -v);
                account_row.start_of_block_balance = row.sender_start_of_block_balance;
                account_row.end_of_block_balance = row.sender_end_of_block_balance;
                self.rows.push(account_row);
            }
        }
        if let Some(receiver_id) = row.receiver_id {
            account_row.account_id = receiver_id;
            account_row.other_account_id = row.sender_id.map(|id| id.to_string());
            self.rows.push(account_row);
        }

        Ok(())
    }

    pub async fn last_block_in_range(
        &self,
        db: &ClickDB,
        start_block: BlockHeight,
        end_block: BlockHeight,
    ) -> BlockHeight {
        let res = db
            .max_in_range("block_height", &self.table, start_block, end_block)
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

pub async fn fetch_transfer_rows_from_legacy_db(
    db: &ClickDB,
    start_block_height: BlockHeight,
    end_block_height: Option<BlockHeight>,
    sender: mpsc::Sender<TransferRow>,
    is_running: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let mut current_block_height = start_block_height;
    let batch_size = 10_000;
    loop {
        if !is_running.load(Ordering::SeqCst) {
            break;
        }
        let to_block_height = if let Some(end_block_height) = end_block_height {
            std::cmp::min(current_block_height + batch_size, end_block_height)
        } else {
            current_block_height + batch_size
        };
        if current_block_height > to_block_height {
            break;
        }
        let res = internal_fetch_transfer_rows_from_legacy_db(
            db,
            current_block_height,
            to_block_height,
            &sender,
            is_running.clone(),
        )
        .await;
        match res {
            Ok(_) => {
                current_block_height = to_block_height;
            }
            Err(err) => {
                tracing::log::error!(
                    target: PROJECT_ID,
                    "Error fetching transfer rows from legacy DB for blocks {} to {}: {}. Retrying in 5 seconds...",
                    current_block_height,
                    to_block_height,
                    err
                );
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        }
    }
    Ok(())
}

async fn internal_fetch_transfer_rows_from_legacy_db(
    db: &ClickDB,
    current_block_height: BlockHeight,
    to_block_height: BlockHeight,
    sender: &mpsc::Sender<TransferRow>,
    is_running: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    tracing::log::info!(
        target: PROJECT_ID,
        "Fetching transfer rows from legacy DB for blocks {} to {}",
        current_block_height,
        to_block_height
    );
    let query = format!(
        "SELECT * FROM transfers WHERE block_height >= {} AND block_height < {} ORDER BY block_height, transfer_index",
        current_block_height, to_block_height
    );
    let mut cursor = db.client.query(&query).fetch::<TransferRow>()?;
    while let Some(row) = cursor.next().await? {
        if !is_running.load(Ordering::SeqCst) {
            break;
        }
        sender.send(row).await?;
    }
    Ok(())
}
