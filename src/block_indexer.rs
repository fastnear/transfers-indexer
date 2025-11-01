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
use reqwest::Client;
use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

const NEAR_BASE_FACTOR: f64 = 1e24;
const ASSET_ID_NATIVE_NEAR: &str = "native:near";
const ACCOUNT_ID_WRAPPED: &str = "wrap.near";
const ASSET_ID_WRAPPED_NEAR: &str = "nep141:wrap.near";
const ACCOUNT_ID_INTENTS: &str = "intents.near";

const ACCOUNT_ID_STNEAR: &str = "meta-pool.near";
const ASSET_ID_STNEAR: &str = "nep141:meta-pool.near";
const ACCOUNT_ID_LINEAR: &str = "linear-protocol.near";
const ASSET_ID_LINEAR: &str = "nep141:linear-protocol.near";
const ACCOUNT_ID_RNEAR: &str = "lst.rhealab.near";
const ASSET_ID_RNEAR: &str = "nep141:lst.rhealab.near";

const EVENT_STANDARD_FT: &str = "nep141";
const EVENT_FT_TRANSFER: &str = "ft_transfer";
const EVENT_FT_MINT: &str = "ft_mint";
const EVENT_FT_BURN: &str = "ft_burn";

const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";

const EVENT_STANDARD_MT: &str = "nep245";
const EVENT_MFT_TRANSFER: &str = "mt_transfer";
const EVENT_MFT_MINT: &str = "mt_mint";
const EVENT_MFT_BURN: &str = "mt_burn";

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
        row.asset_id = ASSET_ID_NATIVE_NEAR.to_string();
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
        let mut pending_row = PendingRow {
            row,
            task_groups: vec![
                TaskGroup::Decimals {
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
        if let Some(task) = self.get_custom_price_task(&pending_row.row.asset_id) {
            pending_row.task_groups.push(TaskGroup::LstPrice {
                lst_price: self.task(task),
            });
        }

        self.pending_rows.push(pending_row);
    }

    fn get_custom_price_task(&mut self, asset_id: &str) -> Option<Task> {
        match asset_id {
            ASSET_ID_STNEAR => Some(Task::CustomViewCall {
                contract_id: ACCOUNT_ID_STNEAR.parse().unwrap(),
                method_name: "get_st_near_price".to_string(),
                arguments: "{}".to_string(),
                block_hash: self.block_hash,
            }),
            ASSET_ID_LINEAR => Some(Task::CustomViewCall {
                contract_id: ACCOUNT_ID_LINEAR.parse().unwrap(),
                method_name: "ft_price".to_string(),
                arguments: "{}".to_string(),
                block_hash: self.block_hash,
            }),
            ASSET_ID_RNEAR => Some(Task::CustomViewCall {
                contract_id: ACCOUNT_ID_RNEAR.parse().unwrap(),
                method_name: "ft_price".to_string(),
                arguments: "{}".to_string(),
                block_hash: self.block_hash,
            }),
            _ => None,
        }
    }

    fn mt_decimal_task(&mut self, mt_transfer: &MtTransfer) -> TaskGroup {
        if mt_transfer.contract_id == ACCOUNT_ID_INTENTS {
            if let Some((token_standard, token_id)) = mt_transfer.token_id.split_once(":") {
                if token_standard == EVENT_STANDARD_FT {
                    if let Ok(contract_id) = AccountId::from_str(token_id) {
                        return TaskGroup::Decimals {
                            decimals: self.task(Task::FtDecimals {
                                contract_id: contract_id.clone(),
                                block_hash: self.block_hash,
                            }),
                        };
                    }
                } else if token_standard == EVENT_STANDARD_MT {
                    if let Some((contract_id, token_id)) = token_id.split_once(":") {
                        if let Ok(contract_id) = AccountId::from_str(contract_id) {
                            return TaskGroup::Decimals {
                                decimals: self.task(Task::MtDecimals {
                                    contract_id: contract_id.clone(),
                                    token_id: token_id.to_string(),
                                    block_hash: self.block_hash,
                                }),
                            };
                        }
                    }
                }
            }
        };
        TaskGroup::Decimals {
            decimals: self.task(Task::MtDecimals {
                contract_id: mt_transfer.contract_id.clone(),
                token_id: mt_transfer.token_id.clone(),
                block_hash: self.block_hash,
            }),
        }
    }

    pub fn add_pending_row_mt(
        &mut self,
        mut row: TransferRow,
        mt_transfer: MtTransfer,
        transfer_type: Option<TransferType>,
    ) {
        row.asset_id = asset_id_from_mt(&mt_transfer.contract_id, &mt_transfer.token_id);
        row.sender_id = mt_transfer.sender_id.as_ref().map(|id| id.to_string());
        row.receiver_id = mt_transfer.receiver_id.as_ref().map(|id| id.to_string());
        row.amount = mt_transfer.amount;
        row.transfer_type = transfer_type
            .unwrap_or(TransferType::MtTransfer)
            .to_string();
        row.asset_type = AssetType::Mt.to_string();

        let mut pending_row = PendingRow {
            row,
            task_groups: vec![
                self.mt_decimal_task(&mt_transfer),
                TaskGroup::BlockBalances {
                    sender_start_of_block_balance: mt_transfer.sender_id.as_ref().map(|a| {
                        self.task(Task::MtBalance {
                            contract_id: mt_transfer.contract_id.clone(),
                            token_id: mt_transfer.token_id.clone(),
                            account_id: a.clone(),
                            block_hash: self.previous_block_hash,
                        })
                    }),
                    sender_end_of_block_balance: mt_transfer.sender_id.as_ref().map(|a| {
                        self.task(Task::MtBalance {
                            contract_id: mt_transfer.contract_id.clone(),
                            token_id: mt_transfer.token_id.clone(),
                            account_id: a.clone(),
                            block_hash: self.block_hash,
                        })
                    }),
                    receiver_start_of_block_balance: mt_transfer.receiver_id.as_ref().map(|a| {
                        self.task(Task::MtBalance {
                            contract_id: mt_transfer.contract_id.clone(),
                            token_id: mt_transfer.token_id.clone(),
                            account_id: a.clone(),
                            block_hash: self.previous_block_hash,
                        })
                    }),
                    receiver_end_of_block_balance: mt_transfer.receiver_id.as_ref().map(|a| {
                        self.task(Task::MtBalance {
                            contract_id: mt_transfer.contract_id.clone(),
                            token_id: mt_transfer.token_id.clone(),
                            account_id: a.clone(),
                            block_hash: self.block_hash,
                        })
                    }),
                },
            ],
        };
        if let Some(task) =
            self.get_custom_price_task(&map_asset_id_to_intents(&pending_row.row.asset_id))
        {
            pending_row.task_groups.push(TaskGroup::LstPrice {
                lst_price: self.task(task),
            });
        }

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
                        let mut transfers: HashMap<Transfer, usize> = HashMap::new();
                        let mut add_transfer = |transfer: Transfer, add: bool| {
                            let v = transfers.entry(transfer).or_default();
                            let prev = *v;
                            *v = if add {
                                prev + 1
                            } else {
                                prev.saturating_sub(1)
                            };
                            prev == 0
                        };

                        // Optimistically find the method_name for logs
                        let mut method_name = None;
                        for action in &actions {
                            match action {
                                ActionView::FunctionCall {
                                    method_name: new_method_name,
                                    ..
                                } => {
                                    let new_method_name = Some(new_method_name.clone());
                                    if method_name.is_some() && new_method_name != method_name {
                                        method_name = None;
                                        break;
                                    }
                                    method_name = new_method_name;
                                }
                                _ => {}
                            }
                        }

                        for (log_index, log) in logs.into_iter().enumerate() {
                            let mut transfer_index = global_transfer_index * TRANSFER_MULTIPLIER;
                            global_transfer_index += 1;

                            if !log.starts_with(EVENT_JSON_PREFIX) {
                                continue;
                            }

                            let event_json = &log[EVENT_JSON_PREFIX.len()..];
                            let event: JsonEvent = match serde_json::from_str(&event_json) {
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
                                method_name: method_name.clone(),
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
                                        add_transfer((&ft_transfer).into(), true);
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
                                        add_transfer((&ft_transfer).into(), true);
                                        self.add_pending_row_ft(row.clone(), ft_transfer, None);
                                    }

                                    continue;
                                }
                            }

                            if event.standard == EVENT_STANDARD_MT
                                && has_transfer_type(&TransferType::MtTransfer)
                            {
                                if event.event == EVENT_MFT_TRANSFER {
                                    for data_value in event.data {
                                        let data: JsonEventMftTransfer =
                                            match serde_json::from_value(data_value) {
                                                Ok(event) => event,
                                                Err(_) => continue,
                                            };
                                        if data.token_ids.len() != data.amounts.len() {
                                            // Invalid event
                                            continue;
                                        }
                                        for (token_id, amount) in
                                            data.token_ids.into_iter().zip(data.amounts.into_iter())
                                        {
                                            row.transfer_index = transfer_index;
                                            transfer_index += 1;
                                            let mt_transfer = MtTransfer {
                                                contract_id: account_id.clone(),
                                                token_id,
                                                sender_id: Some(data.old_owner_id.clone()),
                                                receiver_id: Some(data.new_owner_id.clone()),
                                                amount: amount.0,
                                            };
                                            add_transfer((&mt_transfer).into(), true);
                                            self.add_pending_row_mt(row.clone(), mt_transfer, None);
                                        }
                                    }

                                    continue;
                                } else if event.event == EVENT_MFT_BURN
                                    || event.event == EVENT_MFT_MINT
                                {
                                    for data_value in event.data {
                                        let data: JsonEventMftMintOrBurn =
                                            match serde_json::from_value(data_value) {
                                                Ok(event) => event,
                                                Err(_) => continue,
                                            };
                                        if data.token_ids.len() != data.amounts.len() {
                                            // Invalid event
                                            continue;
                                        }
                                        let (sender_id, receiver_id) =
                                            if event.event == EVENT_MFT_MINT {
                                                (None, Some(data.owner_id))
                                            } else {
                                                (Some(data.owner_id), None)
                                            };
                                        for (token_id, amount) in
                                            data.token_ids.into_iter().zip(data.amounts.into_iter())
                                        {
                                            row.transfer_index = transfer_index;
                                            transfer_index += 1;
                                            let mt_transfer = MtTransfer {
                                                contract_id: account_id.clone(),
                                                token_id,
                                                sender_id: sender_id.clone(),
                                                receiver_id: receiver_id.clone(),
                                                amount: amount.0,
                                            };
                                            add_transfer((&mt_transfer).into(), true);
                                            self.add_pending_row_mt(row.clone(), mt_transfer, None);
                                        }

                                        continue;
                                    }
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
                                        if add_transfer((&ft_transfer).into(), false) {
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
                                        if add_transfer((&ft_transfer).into(), false) {
                                            self.add_pending_row_ft(row, ft_transfer, None);
                                        }
                                        continue;
                                    }

                                    if (account_id.as_str() == ACCOUNT_ID_WRAPPED)
                                        && has_transfer_type(&TransferType::WrappedNear)
                                    {
                                        if method_name == "near_deposit" {
                                            let ft_transfer = FtTransfer {
                                                contract_id: account_id.clone(),
                                                sender_id: None,
                                                receiver_id: Some(predecessor_id.clone()),
                                                amount: deposit,
                                            };
                                            if add_transfer((&ft_transfer).into(), false) {
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
                                            if add_transfer((&ft_transfer).into(), false) {
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
        client: &Client,
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
        let (task_results, intents_tokens) = tokio::try_join!(
            rpc::fetch_from_rpc(client, &tasks, rpc_config),
            rpc::fetch_intents_prices(client, rpc_config, self.block_timestamp, true)
        )?;
        assert_eq!(
            task_results.len(),
            tasks.len(),
            "Invalid number of tasks returned"
        );

        let mut lst_prices: HashMap<String, f64> = HashMap::new();

        let mut rows: Vec<TransferRow> = self
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
                        TaskGroup::Decimals { decimals } => {
                            if let Some(decimals) = resolve_task(&task_results, Some(decimals)) {
                                let factor = 10f64.powi(decimals as i32);
                                row.human_amount = Some(row.amount as f64 / factor);
                            }
                        }
                        TaskGroup::LstPrice { lst_price } => {
                            if let Some(price) = resolve_task(&task_results, Some(lst_price)) {
                                let factor = price.0 as f64 / NEAR_BASE_FACTOR;
                                lst_prices.insert(map_asset_id_to_intents(&row.asset_id), factor);
                            }
                        }
                    }
                }
                row
            })
            .collect();

        if let Some(intents_tokens) = intents_tokens {
            let mut prices: HashMap<String, f64> = intents_tokens
                .into_iter()
                .map(|token| (token.asset_id, token.price))
                .collect();
            if let Some(near_price) = prices.get(ASSET_ID_WRAPPED_NEAR).cloned() {
                for (asset_id, factor) in lst_prices {
                    prices.insert(asset_id, factor * near_price);
                }
            }
            rows.iter_mut().for_each(|row| {
                row.usd_amount = row.human_amount.and_then(|amount| {
                    prices
                        .get(&map_asset_id_to_intents(&row.asset_id))
                        .map(|price| amount * price)
                });
            });
        }

        let mut task_cache = self.task_cache;
        task_cache.retain(|task, _| matches!(task, Task::FtDecimals { .. }));
        task_cache.extend(tasks.into_iter().zip(task_results.into_iter()));

        Ok((rows, task_cache))
    }
}

fn asset_id_from_ft(contract_id: &AccountId) -> String {
    format!("nep141:{}", contract_id)
}

fn asset_id_from_mt(contract_id: &AccountId, token_id: &String) -> String {
    format!("nep245:{}:{}", contract_id, token_id)
}

fn map_asset_id_to_intents(asset_id: &String) -> String {
    if asset_id == ASSET_ID_NATIVE_NEAR {
        return ASSET_ID_WRAPPED_NEAR.to_string();
    }
    if let Some((token_standard, rest)) = asset_id.split_once(":") {
        if token_standard == EVENT_STANDARD_MT {
            if let Some((contract_id, rest)) = rest.split_once(":") {
                if contract_id == ACCOUNT_ID_INTENTS {
                    return rest.to_string();
                }
            }
        }
    }
    asset_id.to_string()
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
