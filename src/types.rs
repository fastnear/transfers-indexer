use clickhouse::Row;
use fastnear_primitives::near_indexer_primitives::types::AccountId;
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use fastnear_primitives::near_primitives::serialize::dec_format;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

pub const TRANSFER_MULTIPLIER: u32 = 10000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub enum TransferType {
    NativeTransfer,
    AttachedDeposit,
    FtTransfer,
    FtTransferCall,
    FtResolveTransfer,
    WrappedNear,
}

impl Display for TransferType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{:?}", self).trim_matches('"'))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AssetType {
    Near,
    /// NEP-141 Fungible Token
    Ft,
}

impl Display for AssetType {
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
            TransferType::WrappedNear => 0,
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
    pub sender_id: Option<String>,
    pub receiver_id: Option<String>,
    pub asset_id: String,
    pub asset_type: String,
    pub amount: u128,
    pub method_name: Option<String>,
    pub transfer_type: String,
    pub human_amount: Option<f64>,
    pub usd_amount: Option<f64>,
    pub sender_start_of_block_balance: Option<u128>,
    pub sender_end_of_block_balance: Option<u128>,
    pub receiver_start_of_block_balance: Option<u128>,
    pub receiver_end_of_block_balance: Option<u128>,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum Task {
    AccountBalance {
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

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub struct TaskId(pub usize);

/// Represents either a task ID or a direct value.
#[derive(Debug, Clone)]
pub enum TaskIdOrValue<V> {
    TaskId(TaskId),
    Value(V),
    ErrorOrMissing,
}

pub enum TaskGroup {
    BlockBalances {
        sender_start_of_block_balance: Option<TaskIdOrValue<u128>>,
        sender_end_of_block_balance: Option<TaskIdOrValue<u128>>,
        receiver_start_of_block_balance: Option<TaskIdOrValue<u128>>,
        receiver_end_of_block_balance: Option<TaskIdOrValue<u128>>,
    },
    FtDecimals {
        decimals: TaskIdOrValue<u8>,
    },
}

pub struct PendingRow {
    pub row: TransferRow,
    pub task_groups: Vec<TaskGroup>,
}

pub type TaskResult = Option<serde_json::Value>;

/// Caches the result of some tasks from the previous blocks
pub type TaskCache = HashMap<Task, TaskResult>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct U128(#[serde(with = "dec_format")] pub u128);

#[derive(Debug, Clone, Deserialize)]
pub struct FtTransferArgs {
    pub receiver_id: AccountId,
    #[serde(with = "dec_format")]
    pub amount: u128,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WNearWithdrawArgs {
    #[serde(with = "dec_format")]
    pub amount: u128,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FtResolveTransferArgs {
    pub sender_id: AccountId,
    pub receiver_id: AccountId,
    #[serde(with = "dec_format")]
    pub amount: u128,
}
