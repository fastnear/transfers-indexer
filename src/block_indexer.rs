use crate::types::*;

use crate::rpc;
use crate::rpc::RpcConfig;
use fastnear_primitives::block_with_tx_hash::BlockWithTxHashes;
use fastnear_primitives::near_indexer_primitives::types::{AccountId, BlockHeight};
use fastnear_primitives::near_indexer_primitives::views::{
    ActionView, ExecutionStatusView, ReceiptEnumView,
};
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use hashlink::LinkedHashMap;
use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};

const NEAR_BASE_FACTOR: f64 = 1e24;
const NATIVE_NEAR_ASSET_ID: &str = "native:near";
const WRAPPED_NEAR_MAINNET: &str = "wrap.near";
const WRAPPED_NEAR_TESTNET: &str = "wrap.testnet";
const EVENT_STANDARD_FT: &str = "nep141";
const EVENT_FT_TRANSFER: &str = "ft_transfer";
const EVENT_FT_MINT: &str = "ft_mint";
const EVENT_FT_BURN: &str = "ft_burn";

pub struct BlockIndexer {
    pub task_cache: TaskCache,
    // Maps a task to its pending task ID
    pub pending_tasks: LinkedHashMap<Task, TaskId>,
    pub pending_rows: Vec<PendingRow>,
    pub block_height: BlockHeight,
    pub block_hash: CryptoHash,
    pub previous_block_hash: CryptoHash,
    pub block_timestamp: u64,
}

impl BlockIndexer {
    pub fn new(task_cache: Option<TaskCache>) -> Self {
        Self {
            task_cache: task_cache.unwrap_or_default(),
            pending_tasks: LinkedHashMap::new(),
            pending_rows: vec![],
            block_height: 0,
            block_hash: CryptoHash::default(),
            previous_block_hash: CryptoHash::default(),
            block_timestamp: 0,
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
        mut row: TransferRow,
        sender_id: Option<&AccountId>,
        receiver_id: Option<&AccountId>,
    ) {
        row.asset_id = NATIVE_NEAR_ASSET_ID.to_string();
        row.human_amount = Some(row.amount as f64 / NEAR_BASE_FACTOR);
        row.sender_id = sender_id.map(|id| id.to_string());
        row.receiver_id = receiver_id.map(|id| id.to_string());
        row.asset_type = AssetType::Near.to_string();
        let pending_row = PendingRow {
            row,
            task_groups: vec![TaskGroup::BlockBalances {
                sender_start_of_block_balance: sender_id.map(|a| {
                    self.task(Task::AccountBalance {
                        account_id: a.clone(),
                        block_hash: self.previous_block_hash,
                    })
                }),
                sender_end_of_block_balance: sender_id.map(|a| {
                    self.task(Task::AccountBalance {
                        account_id: a.clone(),
                        block_hash: self.block_hash,
                    })
                }),
                receiver_start_of_block_balance: receiver_id.map(|a| {
                    self.task(Task::AccountBalance {
                        account_id: a.clone(),
                        block_hash: self.previous_block_hash,
                    })
                }),
                receiver_end_of_block_balance: receiver_id.map(|a| {
                    self.task(Task::AccountBalance {
                        account_id: a.clone(),
                        block_hash: self.block_hash,
                    })
                }),
            }],
        };
        self.pending_rows.push(pending_row);
    }

    pub fn add_pending_row_ft(
        &mut self,
        mut row: TransferRow,
        ft_transfer: FtTransfer,
        transfer_type: Option<TransferType>,
    ) {
        row.asset_id = asset_id_from_ft(&ft_transfer.contract_id);
        row.sender_id = ft_transfer.sender_id.as_ref().map(|id| id.to_string());
        row.receiver_id = ft_transfer.receiver_id.as_ref().map(|id| id.to_string());
        row.amount = ft_transfer.amount;
        row.transfer_type = transfer_type
            .unwrap_or(TransferType::FtTransfer)
            .to_string();
        row.asset_type = AssetType::Ft.to_string();
        let pending_row = PendingRow {
            row,
            task_groups: vec![
                TaskGroup::FtDecimals {
                    decimals: self.task(Task::FtDecimals {
                        contract_id: ft_transfer.contract_id.clone(),
                        block_hash: self.block_hash,
                    }),
                },
                TaskGroup::BlockBalances {
                    sender_start_of_block_balance: ft_transfer.sender_id.as_ref().map(|a| {
                        self.task(Task::FtBalance {
                            contract_id: ft_transfer.contract_id.clone(),
                            account_id: a.clone(),
                            block_hash: self.previous_block_hash,
                        })
                    }),
                    sender_end_of_block_balance: ft_transfer.sender_id.as_ref().map(|a| {
                        self.task(Task::FtBalance {
                            contract_id: ft_transfer.contract_id.clone(),
                            account_id: a.clone(),
                            block_hash: self.block_hash,
                        })
                    }),
                    receiver_start_of_block_balance: ft_transfer.receiver_id.as_ref().map(|a| {
                        self.task(Task::FtBalance {
                            contract_id: ft_transfer.contract_id.clone(),
                            account_id: a.clone(),
                            block_hash: self.previous_block_hash,
                        })
                    }),
                    receiver_end_of_block_balance: ft_transfer.receiver_id.as_ref().map(|a| {
                        self.task(Task::FtBalance {
                            contract_id: ft_transfer.contract_id.clone(),
                            account_id: a.clone(),
                            block_hash: self.block_hash,
                        })
                    }),
                },
            ],
        };
        self.pending_rows.push(pending_row);
    }

    pub fn process_block(
        &mut self,
        block: BlockWithTxHashes,
        transfer_types: &Option<HashSet<TransferType>>,
    ) -> anyhow::Result<()> {
        self.block_height = block.block.header.height;
        self.block_hash = block.block.header.hash;
        self.previous_block_hash = block.block.header.prev_hash;
        self.block_timestamp = block.block.header.timestamp_nanosec;

        let has_transfer_type = |tt: &TransferType| {
            if let Some(transfer_types) = transfer_types {
                transfer_types.contains(tt)
            } else {
                true
            }
        };

        let mut global_transfer_index: u32 = 0;
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
                        let logs = reo.execution_outcome.outcome.logs;
                        if matches!(status, ExecutionStatusView::Failure(_)) {
                            global_transfer_index += (logs.len() + actions.len()) as u32;
                            continue;
                        }

                        // Parse FT logs and remember them, so we can ignore method name FT
                        // transfers later.
                        let mut ft_transfers: HashMap<FtTransfer, usize> = HashMap::new();
                        let mut add_ft_transfer = |ft_transfer: &FtTransfer, add: bool| {
                            let v = ft_transfers.entry(ft_transfer.clone()).or_default();
                            let prev = *v;
                            *v = if add {
                                prev + 1
                            } else {
                                prev.saturating_sub(1)
                            };
                            prev == 0
                        };

                        for (log_index, log) in logs.into_iter().enumerate() {
                            let mut transfer_index = global_transfer_index * TRANSFER_MULTIPLIER;
                            global_transfer_index += 1;

                            let event: JsonEvent = match serde_json::from_str(&log) {
                                Ok(event) => event,
                                Err(_) => continue,
                            };

                            let mut row = TransferRow {
                                block_height: self.block_height,
                                block_timestamp: self.block_timestamp,
                                transaction_id: transaction_id.map(|h| h.to_string()),
                                receipt_id: receipt_id.to_string(),
                                action_index: None,
                                log_index: Some(log_index as _),
                                transfer_index,
                                signer_id: signer_id.to_string(),
                                predecessor_id: predecessor_id.to_string(),
                                account_id: account_id.to_string(),
                                sender_id: None,
                                receiver_id: None,
                                asset_id: "".to_string(),
                                asset_type: "".to_string(),
                                amount: 0,
                                method_name: None,
                                transfer_type: "".to_string(),
                                human_amount: None,
                                usd_amount: None,
                                sender_start_of_block_balance: None,
                                sender_end_of_block_balance: None,
                                receiver_start_of_block_balance: None,
                                receiver_end_of_block_balance: None,
                            };

                            if event.standard == EVENT_STANDARD_FT
                                && has_transfer_type(&TransferType::FtTransfer)
                            {
                                if event.event == EVENT_FT_TRANSFER {
                                    for data_value in event.data {
                                        row.transfer_index = transfer_index;
                                        transfer_index += 1;
                                        let data: JsonEventFtTransfer =
                                            match serde_json::from_value(data_value) {
                                                Ok(event) => event,
                                                Err(_) => continue,
                                            };
                                        let ft_transfer = FtTransfer {
                                            contract_id: account_id.clone(),
                                            sender_id: Some(data.old_owner_id.clone()),
                                            receiver_id: Some(data.new_owner_id.clone()),
                                            amount: data.amount,
                                        };
                                        add_ft_transfer(&ft_transfer, true);
                                        self.add_pending_row_ft(row.clone(), ft_transfer, None);
                                    }

                                    continue;
                                } else if event.event == EVENT_FT_BURN
                                    || event.event == EVENT_FT_MINT
                                {
                                    for data_value in event.data {
                                        row.transfer_index = transfer_index;
                                        transfer_index += 1;
                                        let data: JsonEventFtMintOrBurn =
                                            match serde_json::from_value(data_value) {
                                                Ok(event) => event,
                                                Err(_) => continue,
                                            };
                                        let (sender_id, receiver_id) =
                                            if event.event == EVENT_FT_MINT {
                                                (None, Some(data.owner_id))
                                            } else {
                                                (Some(data.owner_id), None)
                                            };
                                        let ft_transfer = FtTransfer {
                                            contract_id: account_id.clone(),
                                            sender_id,
                                            receiver_id,
                                            amount: data.amount,
                                        };
                                        add_ft_transfer(&ft_transfer, true);
                                        self.add_pending_row_ft(row.clone(), ft_transfer, None);
                                    }

                                    continue;
                                }
                            }
                        }

                        for (action_index, action) in actions.into_iter().enumerate() {
                            let transfer_index = global_transfer_index * TRANSFER_MULTIPLIER;
                            global_transfer_index += 1;

                            let mut row = TransferRow {
                                block_height: self.block_height,
                                block_timestamp: self.block_timestamp,
                                transaction_id: transaction_id.map(|h| h.to_string()),
                                receipt_id: receipt_id.to_string(),
                                action_index: Some(action_index as u16),
                                log_index: None,
                                transfer_index,
                                signer_id: signer_id.to_string(),
                                predecessor_id: predecessor_id.to_string(),
                                account_id: account_id.to_string(),
                                sender_id: None,
                                receiver_id: None,
                                asset_id: "".to_string(),
                                asset_type: "".to_string(),
                                amount: 0,
                                method_name: None,
                                transfer_type: "".to_string(),
                                human_amount: None,
                                usd_amount: None,
                                sender_start_of_block_balance: None,
                                sender_end_of_block_balance: None,
                                receiver_start_of_block_balance: None,
                                receiver_end_of_block_balance: None,
                            };

                            match action {
                                ActionView::Transfer { deposit } => {
                                    if !has_transfer_type(&TransferType::NativeTransfer) {
                                        continue;
                                    }

                                    row.amount = deposit;
                                    row.transfer_type = TransferType::NativeTransfer.to_string();
                                    row.transfer_index = transfer_index
                                        + TransferType::NativeTransfer.transfer_index_offset();
                                    self.add_pending_row_native_near(
                                        row,
                                        Some(&predecessor_id),
                                        Some(&account_id),
                                    );
                                }
                                ActionView::FunctionCall {
                                    method_name,
                                    args,
                                    deposit,
                                    ..
                                } => {
                                    row.method_name = Some(method_name.to_string());
                                    // Ignore yoctoNEAR deposits
                                    if deposit > 1
                                        && has_transfer_type(&TransferType::AttachedDeposit)
                                    {
                                        let mut row = row.clone();
                                        row.transfer_index = transfer_index
                                            + TransferType::AttachedDeposit.transfer_index_offset();
                                        row.amount = deposit;
                                        row.transfer_type =
                                            TransferType::AttachedDeposit.to_string();
                                        self.add_pending_row_native_near(
                                            row,
                                            Some(&predecessor_id),
                                            Some(&account_id),
                                        );
                                    }

                                    if method_name == "ft_transfer"
                                        || method_name == "ft_transfer_call"
                                            && has_transfer_type(&TransferType::FtTransfer)
                                    {
                                        let args = serde_json::from_slice::<FtTransferArgs>(&args);
                                        if args.is_err() {
                                            continue;
                                        }
                                        let args = args.unwrap();

                                        let ft_transfer = FtTransfer {
                                            contract_id: account_id.clone(),
                                            sender_id: Some(predecessor_id.clone()),
                                            receiver_id: Some(args.receiver_id),
                                            amount: args.amount,
                                        };
                                        if add_ft_transfer(&ft_transfer, false) {
                                            self.add_pending_row_ft(row, ft_transfer, None);
                                        }
                                        continue;
                                    }

                                    if method_name == "ft_resolve_transfer"
                                        && has_transfer_type(&TransferType::FtTransfer)
                                    {
                                        let args =
                                            serde_json::from_slice::<FtResolveTransferArgs>(&args);
                                        if args.is_err() {
                                            continue;
                                        }
                                        let args = args.unwrap();

                                        let transferred_amount = match &status {
                                            ExecutionStatusView::SuccessValue(v) => {
                                                let parsed_return_amount =
                                                    serde_json::from_slice::<U128>(&v);
                                                if parsed_return_amount.is_err() {
                                                    continue;
                                                }
                                                parsed_return_amount.unwrap().0
                                            }
                                            _ => continue,
                                        };

                                        let refund_amount =
                                            args.amount.saturating_sub(transferred_amount);
                                        if refund_amount == 0 {
                                            continue;
                                        }

                                        let ft_transfer = FtTransfer {
                                            contract_id: account_id.clone(),
                                            sender_id: Some(args.receiver_id),
                                            receiver_id: Some(args.sender_id),
                                            amount: refund_amount,
                                        };
                                        if add_ft_transfer(&ft_transfer, false) {
                                            self.add_pending_row_ft(row, ft_transfer, None);
                                        }
                                        continue;
                                    }

                                    if (account_id.as_str() == WRAPPED_NEAR_MAINNET
                                        || account_id.as_str() == WRAPPED_NEAR_TESTNET)
                                        && has_transfer_type(&TransferType::WrappedNear)
                                    {
                                        if method_name == "near_deposit" {
                                            let ft_transfer = FtTransfer {
                                                contract_id: account_id.clone(),
                                                sender_id: None,
                                                receiver_id: Some(predecessor_id.clone()),
                                                amount: deposit,
                                            };
                                            if add_ft_transfer(&ft_transfer, false) {
                                                self.add_pending_row_ft(
                                                    row,
                                                    ft_transfer,
                                                    Some(TransferType::WrappedNear),
                                                );
                                            }
                                            continue;
                                        }

                                        if method_name == "near_withdraw" {
                                            let args =
                                                serde_json::from_slice::<WNearWithdrawArgs>(&args);
                                            if args.is_err() {
                                                continue;
                                            }
                                            let args = args.unwrap();

                                            let ft_transfer = FtTransfer {
                                                contract_id: account_id.clone(),
                                                sender_id: Some(predecessor_id.clone()),
                                                receiver_id: None,
                                                amount: args.amount,
                                            };
                                            if add_ft_transfer(&ft_transfer, false) {
                                                self.add_pending_row_ft(
                                                    row,
                                                    ft_transfer,
                                                    Some(TransferType::WrappedNear),
                                                );
                                            }
                                            continue;
                                        }
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

    pub async fn execute_tasks(
        self,
        rpc_config: &RpcConfig,
    ) -> anyhow::Result<(Vec<TransferRow>, TaskCache)> {
        if self.pending_rows.is_empty() {
            return Ok(Default::default());
        }

        let tasks = self
            .pending_tasks
            .into_iter()
            .map(|(k, _v)| k)
            .collect::<Vec<_>>();
        let task_results = rpc::fetch_from_rpc(&tasks, rpc_config).await?;

        let rows = self
            .pending_rows
            .into_iter()
            .map(|pending_row| {
                let mut row = pending_row.row;
                for task_group in pending_row.task_groups {
                    match task_group {
                        TaskGroup::BlockBalances {
                            sender_start_of_block_balance,
                            sender_end_of_block_balance,
                            receiver_start_of_block_balance,
                            receiver_end_of_block_balance,
                        } => {
                            row.sender_start_of_block_balance =
                                resolve_task(&task_results, sender_start_of_block_balance)
                                    .map(|v| v.0);
                            row.sender_end_of_block_balance =
                                resolve_task(&task_results, sender_end_of_block_balance)
                                    .map(|v| v.0);
                            row.receiver_start_of_block_balance =
                                resolve_task(&task_results, receiver_start_of_block_balance)
                                    .map(|v| v.0);
                            row.receiver_end_of_block_balance =
                                resolve_task(&task_results, receiver_end_of_block_balance)
                                    .map(|v| v.0);
                        }
                        TaskGroup::FtDecimals { decimals } => {
                            if let Some(decimals) = resolve_task(&task_results, Some(decimals)) {
                                let factor = 10f64.powi(decimals as i32);
                                row.human_amount = Some(row.amount as f64 / factor);
                            }
                        }
                    }
                }
                row
            })
            .collect();

        let mut task_cache = self.task_cache;
        task_cache.retain(|task, _| matches!(task, Task::FtDecimals { .. }));
        task_cache.extend(tasks.into_iter().zip(task_results.into_iter()));

        Ok((rows, task_cache))
    }
}

fn asset_id_from_ft(contract_id: &AccountId) -> String {
    format!("nep141:{}", contract_id)
}

fn resolve_task<V: DeserializeOwned>(
    task_results: &[TaskResult],
    task_id_or_value: Option<TaskIdOrValue<V>>,
) -> Option<V> {
    match task_id_or_value {
        Some(TaskIdOrValue::Value(v)) => Some(v),
        Some(TaskIdOrValue::TaskId(task_id)) => {
            let result = &task_results[task_id.0];
            match result {
                Some(value) => match serde_json::from_value::<V>(value.clone()) {
                    Ok(v) => Some(v),
                    Err(_) => None,
                },
                None => None,
            }
        }
        _ => None,
    }
}
