use crate::*;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;

use clickhouse::Row;
use fastnear_primitives::near_indexer_primitives::types::AccountId;
use fastnear_primitives::near_indexer_primitives::views::ReceiptEnumView;
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use fastnear_primitives::near_primitives::serialize::dec_format;
use fastnear_primitives::near_primitives::types::BlockHeight;
use fastnear_primitives::near_primitives::views::{ActionView, ExecutionStatusView};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const TRANSFER_MULTIPLIER: u32 = 10000;
const NEAR_BASE_FACTOR: f64 = 1e24;
const NATIVE_NEAR_ASSET_ID: &str = "native:near";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub enum TransferType {
    NativeTransfer,
    AttachedDeposit,
    FtTransfer,
    FtTransferCall,
    FtResolveTransfer,
}

impl Display for TransferType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{:?}", self).trim_matches('"'))
    }
}

impl TransferType {
    pub fn transfer_index_offset(&self) -> u32 {
        match self {
            TransferType::NativeTransfer => TRANSFER_MULTIPLIER - 1,
            TransferType::AttachedDeposit => TRANSFER_MULTIPLIER - 2,
            TransferType::FtTransfer => 0,
            TransferType::FtTransferCall => 0,
            TransferType::FtResolveTransfer => 0,
        }
    }
}

#[derive(Debug, Clone, Row, Serialize)]
pub struct TransferRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub transaction_id: Option<String>,
    pub receipt_id: String,
    pub action_index: u16,
    pub transfer_index: u32,
    pub signer_id: String,
    pub predecessor_id: String,
    pub account_id: String,
    pub sender_id: String,
    pub receiver_id: String,
    pub asset_id: String,
    pub amount: u128,
    pub method_name: Option<String>,
    pub transfer_type: String,
    pub human_amount: Option<f64>,
    pub usd_amount: Option<f64>,
    pub sender_start_of_block_balance: u128,
    pub sender_end_of_block_balance: u128,
    pub receiver_start_of_block_balance: u128,
    pub receiver_end_of_block_balance: u128,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum Task {
    BlockAccountBalance {
        account_id: AccountId,
        block_hash: CryptoHash,
    },
    FtBalance {
        contract_id: AccountId,
        account_id: AccountId,
        block_hash: CryptoHash,
    },
    FtDecimals {
        contract_id: AccountId,
        block_hash: CryptoHash,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct TaskId(pub usize);

/// Represents either a task ID or a direct value.
#[derive(Debug, Clone)]
pub enum TaskIdOrValue<V> {
    TaskId(TaskId),
    Value(V),
    ErrorOrMissing,
}

pub enum TaskGroup {
    NativeNear {
        sender_start_of_block_balance: TaskIdOrValue<u128>,
        sender_end_of_block_balance: TaskIdOrValue<u128>,
        receiver_start_of_block_balance: TaskIdOrValue<u128>,
        receiver_end_of_block_balance: TaskIdOrValue<u128>,
    },
    Ft {
        decimals: TaskIdOrValue<u8>,
        sender_start_of_block_balance: TaskIdOrValue<u128>,
        sender_end_of_block_balance: TaskIdOrValue<u128>,
        receiver_start_of_block_balance: TaskIdOrValue<u128>,
        receiver_end_of_block_balance: TaskIdOrValue<u128>,
    },
}

pub struct PendingRow {
    pub row: TransferRow,
    pub task_group: TaskGroup,
}

/// Caches the result of some tasks from the previous blocks
pub type TaskCache = HashMap<Task, Option<serde_json::Value>>;

pub struct BlockIndexer {
    pub task_cache: TaskCache,
    // Maps a task to its pending task ID
    pub pending_tasks: HashMap<Task, TaskId>,
    pub pending_rows: Vec<PendingRow>,
}

impl BlockIndexer {
    pub fn new(task_cache: Option<TaskCache>) -> Self {
        Self {
            task_cache: task_cache.unwrap_or_default(),
            pending_tasks: HashMap::new(),
            pending_rows: vec![],
        }
    }

    pub fn task<V: DeserializeOwned>(&mut self, task: Task) -> TaskIdOrValue<V> {
        if let Some(cached_value) = self.task_cache.get(&task) {
            if let Some(value) = cached_value {
                match serde_json::from_value::<V>(value.clone()) {
                    Err(_) => return TaskIdOrValue::ErrorOrMissing,
                    Ok(v) => TaskIdOrValue::Value(v),
                }
            } else {
                TaskIdOrValue::ErrorOrMissing
            }
        } else {
            if let Some(task_id) = self.pending_tasks.get(&task) {
                return TaskIdOrValue::TaskId(*task_id);
            }
            let task_id = TaskId(self.pending_tasks.len());
            self.pending_tasks.insert(task, task_id.clone());
            TaskIdOrValue::TaskId(task_id)
        }
    }

    pub fn add_pending_row_native_near(
        &mut self,
        row: TransferRow,
        sender_id: &AccountId,
        receiver_id: &AccountId,
        block_hash: CryptoHash,
        previous_block_hash: CryptoHash,
    ) {
        let pending_row = PendingRow {
            row,
            task_group: TaskGroup::NativeNear {
                sender_start_of_block_balance: self.task(Task::BlockAccountBalance {
                    account_id: sender_id.clone(),
                    block_hash: previous_block_hash,
                }),
                sender_end_of_block_balance: self.task(Task::BlockAccountBalance {
                    account_id: sender_id.clone(),
                    block_hash,
                }),
                receiver_start_of_block_balance: self.task(Task::BlockAccountBalance {
                    account_id: receiver_id.clone(),
                    block_hash: previous_block_hash,
                }),
                receiver_end_of_block_balance: self.task(Task::BlockAccountBalance {
                    account_id: receiver_id.clone(),
                    block_hash,
                }),
            },
        };
        self.pending_rows.push(pending_row);
    }

    pub fn add_pending_row_ft(
        &mut self,
        row: TransferRow,
        contract_id: &AccountId,
        sender_id: &AccountId,
        receiver_id: &AccountId,
        block_hash: CryptoHash,
        previous_block_hash: CryptoHash,
    ) {
        let pending_row = PendingRow {
            row,
            task_group: TaskGroup::Ft {
                decimals: self.task(Task::FtDecimals {
                    contract_id: contract_id.clone(),
                    block_hash,
                }),
                sender_start_of_block_balance: self.task(Task::FtBalance {
                    contract_id: contract_id.clone(),
                    account_id: sender_id.clone(),
                    block_hash: previous_block_hash,
                }),
                sender_end_of_block_balance: self.task(Task::FtBalance {
                    contract_id: contract_id.clone(),
                    account_id: sender_id.clone(),
                    block_hash,
                }),
                receiver_start_of_block_balance: self.task(Task::FtBalance {
                    contract_id: contract_id.clone(),
                    account_id: receiver_id.clone(),
                    block_hash: previous_block_hash,
                }),
                receiver_end_of_block_balance: self.task(Task::FtBalance {
                    contract_id: contract_id.clone(),
                    account_id: receiver_id.clone(),
                    block_hash,
                }),
            },
        };
        self.pending_rows.push(pending_row);
    })

    pub fn process_block(
        &mut self,
        block: BlockWithTxHashes,
        transfer_types: &HashSet<TransferType>,
    ) -> anyhow::Result<()> {
        let block_height = block.block.header.height;
        let block_hash = block.block.header.hash;
        let previous_block_hash = block.block.header.prev_hash;
        let block_timestamp = block.block.header.timestamp_nanosec;
        let mut global_action_index: u32 = 0;
        for shard in block.shards {
            for reo in shard.receipt_execution_outcomes {
                let transaction_id = reo.tx_hash;
                let receipt_id = reo.receipt.receipt_id;
                let predecessor_id = reo.receipt.predecessor_id;
                let account_id = reo.receipt.receiver_id;
                let status = reo.execution_outcome.outcome.status;
                match reo.receipt.receipt {
                    ReceiptEnumView::Action {
                        signer_id, actions, ..
                    } => {
                        if matches!(status, ExecutionStatusView::Failure(_)) {
                            global_action_index += actions.len() as u32;
                            continue;
                        }
                        for (action_index, action) in actions.into_iter().enumerate() {
                            let transfer_index = global_action_index * TRANSFER_MULTIPLIER;
                            let mut row = TransferRow {
                                block_height,
                                block_timestamp,
                                transaction_id: transaction_id.map(|h| h.to_string()),
                                receipt_id: receipt_id.to_string(),
                                action_index: action_index as u16,
                                transfer_index,
                                signer_id: signer_id.to_string(),
                                predecessor_id: predecessor_id.to_string(),
                                account_id: account_id.to_string(),
                                sender_id: "".to_string(),
                                receiver_id: "".to_string(),
                                asset_id: "".to_string(),
                                amount: 0,
                                method_name: None,
                                transfer_type: "".to_string(),
                                human_amount: None,
                                usd_amount: None,
                                sender_start_of_block_balance: 0,
                                sender_end_of_block_balance: 0,
                                receiver_start_of_block_balance: 0,
                                receiver_end_of_block_balance: 0,
                            };

                            global_action_index += 1;
                            match action {
                                ActionView::Transfer { deposit } => {
                                    if !transfer_types.contains(&TransferType::NativeTransfer) {
                                        continue;
                                    }

                                    row.asset_id = NATIVE_NEAR_ASSET_ID.to_string();
                                    row.amount = deposit;
                                    row.human_amount = Some(deposit as f64 / NEAR_BASE_FACTOR);
                                    row.transfer_type = TransferType::NativeTransfer.to_string();
                                    row.sender_id = predecessor_id.to_string();
                                    row.receiver_id = account_id.to_string();
                                    row.transfer_index = transfer_index
                                        + TransferType::NativeTransfer.transfer_index_offset();
                                    self.add_pending_row_native_near(
                                        row,
                                        &predecessor_id,
                                        &account_id,
                                        block_hash,
                                        previous_block_hash,
                                    );
                                }
                                ActionView::FunctionCall {
                                    method_name,
                                    args,
                                    deposit,
                                    ..
                                } => {
                                    // Ignore yoctoNEAR deposits
                                    if deposit > 1
                                        && transfer_types.contains(&TransferType::AttachedDeposit)
                                    {
                                        let mut row = row.clone();
                                        row.transfer_index = transfer_index
                                            + TransferType::AttachedDeposit.transfer_index_offset();
                                        row.asset_id = NATIVE_NEAR_ASSET_ID.to_string();
                                        row.amount = deposit;
                                        row.human_amount = Some(deposit as f64 / NEAR_BASE_FACTOR);
                                        row.transfer_type =
                                            TransferType::AttachedDeposit.to_string();
                                        row.method_name = Some(method_name.clone());
                                        row.sender_id = predecessor_id.to_string();
                                        row.receiver_id = account_id.to_string();
                                        self.add_pending_row_native_near(
                                            row,
                                            &predecessor_id,
                                            &account_id,
                                            block_hash,
                                            previous_block_hash,
                                        );
                                    }

                                    if method_name == "ft_transfer"
                                        && transfer_types.contains(&TransferType::FtTransfer)
                                    {
                                        let args = serde_json::from_slice::<FtTransferArgs>(&args);
                                        if args.is_err() {
                                            continue;
                                        }
                                        let args = args.unwrap();
                                        row.asset_id = asset_id_from_ft(&account_id);
                                        row.amount = args.amount;
                                        row.transfer_type = TransferType::FtTransfer.to_string();
                                        row.method_name = Some(method_name.clone());
                                        row.sender_id = predecessor_id.to_string();
                                        row.receiver_id = args.receiver_id.to_string();
                                        self.add_pending_row_ft(
                                            row,
                                            &account_id,
                                            &predecessor_id,
                                            &args.receiver_id,
                                            block_hash,
                                            previous_block_hash,
                                        );
                                        continue;
                                    }

                                    if method_name == "ft_transfer_call"
                                        && transfer_types.contains(&TransferType::FtTransferCall)
                                    {
                                        let args = serde_json::from_slice::<FtTransferArgs>(&args);
                                        if args.is_err() {
                                            continue;
                                        }
                                        let args = args.unwrap();
                                        row.asset_id = asset_id_from_ft(&account_id);
                                        row.amount = args.amount;
                                        row.transfer_type =
                                            TransferType::FtTransferCall.to_string();
                                        row.method_name = Some(method_name.clone());
                                        row.sender_id = predecessor_id.to_string();
                                        row.receiver_id = args.receiver_id.to_string();
                                        self.add_pending_row_ft(
                                            row,
                                            &account_id,
                                            &predecessor_id,
                                            &args.receiver_id,
                                            block_hash,
                                            previous_block_hash,
                                        );
                                        continue;
                                    }

                                    if method_name == "ft_resolve_transfer"
                                        && transfer_types.contains(&TransferType::FtResolveTransfer)
                                    {
                                        let args = serde_json::from_slice::<FtResolveTransferArgs>(&args);
                                        if args.is_err() {
                                            continue;
                                        }
                                        let args = args.unwrap();

                                        let transferred_amount = match &status {
                                            ExecutionStatusView::SuccessValue(v) => {
                                                let parsed_return_amount = serde_json::from_slice::<U128>(&v);
                                                if parsed_return_amount.is_err() {
                                                    continue;
                                                }
                                                parsed_return_amount.unwrap().0
                                            },
                                            _ => continue,
                                        };

                                        let refund_amount = args.amount.saturating_sub(transferred_amount);
                                        if refund_amount == 0 {
                                            continue;
                                        }

                                        row.asset_id = asset_id_from_ft(&account_id);
                                        row.amount = refund_amount;
                                        row.transfer_type =
                                            TransferType::FtResolveTransfer.to_string();
                                        row.method_name = Some(method_name.clone());
                                        // Swap sender and receiver for the refund
                                        row.sender_id = args.receiver_id.to_string();
                                        row.receiver_id = args.sender_id.to_string();
                                        self.add_pending_row_ft(
                                            row,
                                            &account_id,
                                            &args.receiver_id,
                                            &args.sender_id,
                                            block_hash,
                                            previous_block_hash,
                                        );
                                        continue;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

pub struct TransfersIndexer {
    pub commit_every_block: bool,
    pub rows: Vec<TransferRow>,
    pub commit_handlers: Vec<tokio::task::JoinHandle<Result<(), clickhouse::error::Error>>>,
    pub transfer_types: HashSet<TransferType>,
    pub task_cache: Option<TaskCache>,
}

impl TransfersIndexer {
    pub fn new(transfer_types: &[TransferType]) -> Self {
        let commit_every_block = std::env::var("COMMIT_EVERY_BLOCK")
            .map(|v| v == "true")
            .unwrap_or(false);
        Self {
            commit_every_block,
            rows: vec![],
            commit_handlers: vec![],
            transfer_types: transfer_types.iter().copied().collect(),
            task_cache: None,
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
        db: &mut ClickDB,
        mut block: BlockWithTxHashes,
    ) -> anyhow::Result<()> {
        block.shards.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));

        let block_height = block.block.header.height;
        let block_hash = block.block.header.hash;
        let previous_block_hash = block.block.header.prev_hash;
        let block_timestamp = block.block.header.timestamp_nanosec;

        let mut block_indexer = BlockIndexer::new(self.task_cache.take());
        block_indexer.process_block(block, &self.transfer_types)?;

        let is_round_block = block_height % SAVE_STEP == 0;
        if is_round_block {
            tracing::log::info!(target: CLICKHOUSE_TARGET, "#{}: Having {} rows", block_height, self.rows.len());
        }

        self.maybe_commit(db, block_height).await?;
        Ok(())
    }

    pub async fn last_block_height(&mut self, db: &ClickDB) -> BlockHeight {
        db.max("block_height", "transfers").await.unwrap_or(0)
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        while let Some(handler) = self.commit_handlers.pop() {
            handler.await??;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct U128(
    #[serde(with = "dec_format")]
    pub u128);

#[derive(Debug, Clone, Deserialize)]
pub struct FtTransferArgs {
    pub receiver_id: AccountId,
    #[serde(with = "dec_format")]
    pub amount: u128,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FtResolveTransferArgs{
    pub sender_id: AccountId,
    pub receiver_id: AccountId,
    #[serde(with = "dec_format")]
    pub amount: u128,
}

fn asset_id_from_ft(contract_id: &AccountId) -> String {
    format!("nep141:{}", contract_id)
}
