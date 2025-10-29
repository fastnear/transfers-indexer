use clickhouse::Row;
use derivative::Derivative;
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
    WrappedNear,
    MtTransfer,
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
    /// NEP-245 Multi Token
    Mt,
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
            TransferType::WrappedNear => 0,
            TransferType::MtTransfer => 0,
        }
    }
}

#[derive(Debug, Clone, Row, Serialize)]
pub struct TransferRow {
    pub block_height: u64,
    pub block_timestamp: u64,
    pub transaction_id: Option<String>,
    pub receipt_id: String,
    pub action_index: Option<u16>,
    pub log_index: Option<u16>,
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

#[derive(Derivative)]
#[derivative(Debug, Clone, Eq, Hash, PartialEq)]
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
        #[derivative(PartialEq = "ignore")]
        #[derivative(Hash = "ignore")]
        block_hash: CryptoHash,
    },
    MtBalance {
        contract_id: AccountId,
        token_id: String,
        account_id: AccountId,
        block_hash: CryptoHash,
    },
    MtDecimals {
        contract_id: AccountId,
        token_id: String,
        #[derivative(PartialEq = "ignore")]
        #[derivative(Hash = "ignore")]
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
        sender_start_of_block_balance: Option<TaskIdOrValue<U128>>,
        sender_end_of_block_balance: Option<TaskIdOrValue<U128>>,
        receiver_start_of_block_balance: Option<TaskIdOrValue<U128>>,
        receiver_end_of_block_balance: Option<TaskIdOrValue<U128>>,
    },
    Decimals {
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

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct FtTransfer {
    pub contract_id: AccountId,
    pub sender_id: Option<AccountId>,
    pub receiver_id: Option<AccountId>,
    pub amount: u128,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct MtTransfer {
    pub contract_id: AccountId,
    pub token_id: String,
    pub sender_id: Option<AccountId>,
    pub receiver_id: Option<AccountId>,
    pub amount: u128,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum Transfer {
    Ft(FtTransfer),
    Mft(MtTransfer),
}

impl From<&FtTransfer> for Transfer {
    fn from(ft_transfer: &FtTransfer) -> Self {
        Self::Ft(ft_transfer.clone())
    }
}

impl From<&MtTransfer> for Transfer {
    fn from(mt_transfer: &MtTransfer) -> Self {
        Self::Mft(mt_transfer.clone())
    }
}

#[allow(unused)]
#[derive(Debug, Clone, Deserialize)]
pub struct JsonEvent {
    pub version: String,
    pub standard: String,
    pub event: String,
    pub data: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonEventFtTransfer {
    pub old_owner_id: AccountId,
    pub new_owner_id: AccountId,
    #[serde(with = "dec_format")]
    pub amount: u128,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonEventFtMintOrBurn {
    pub owner_id: AccountId,
    #[serde(with = "dec_format")]
    pub amount: u128,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonEventMftTransfer {
    pub old_owner_id: AccountId,
    pub new_owner_id: AccountId,
    pub token_ids: Vec<String>,
    pub amounts: Vec<U128>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonEventMftMintOrBurn {
    pub owner_id: AccountId,
    pub token_ids: Vec<String>,
    pub amounts: Vec<U128>,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntentsTokenResponse {
    /// Unique asset identifier
    #[serde(rename = "assetId")]
    pub asset_id: String,
    /// Number of decimals for the token
    #[serde(rename = "decimals")]
    pub decimals: u8,
    /// Blockchain associated with the token
    #[serde(rename = "blockchain")]
    pub blockchain: String,
    /// Token symbol (e.g. BTC, ETH)
    #[serde(rename = "symbol")]
    pub symbol: String,
    /// Current price of the token in USD
    #[serde(rename = "price")]
    pub price: f64,
    /// Date when the token price was last updated
    #[serde(rename = "priceUpdatedAt")]
    pub price_updated_at: String,
    /// Contract address of the token
    #[serde(rename = "contractAddress", skip_serializing_if = "Option::is_none")]
    pub contract_address: Option<String>,
}
