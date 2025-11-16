use crate::types::*;
use base64::prelude::*;
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use fastnear_primitives::near_primitives::serialize::dec_format;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::env;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task;

const RPC_TIMEOUT: Duration = Duration::from_millis(5000);
const TARGET_RPC: &str = "rpc";
const RPC_ERROR_UNKNOWN_BLOCK: &str = "UNKNOWN_BLOCK";
const RPC_ERROR_UNAVAILABLE_SHARD: &str = "UNAVAILABLE_SHARD";
const INTENTS_TOKENS_URL: &str = "https://1click.chaindefuser.com/v0/tokens";

#[allow(dead_code)]
#[derive(Debug)]
pub enum RpcError {
    ReqwestError(reqwest::Error),
    InvalidFunctionCallResponse(serde_json::Error),
    InvalidAccountStateResponse(serde_json::Error),
    RetriableRpcError(String),
}

impl From<reqwest::Error> for RpcError {
    fn from(error: reqwest::Error) -> Self {
        RpcError::ReqwestError(error)
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for RpcError {}

#[derive(Serialize)]
struct JsonRequest {
    jsonrpc: String,
    method: String,
    params: Value,
    id: String,
}

#[derive(Deserialize)]
struct JsonResponse {
    // id: String,
    // jsonrpc: String,
    result: Option<Value>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize, Debug)]
struct JsonRpcErrorCause {
    // info: Option<Value>,
    name: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct JsonRpcError {
    cause: Option<JsonRpcErrorCause>,
    code: i64,
    data: Option<String>,
}

#[derive(Deserialize)]
struct FunctionCallResponse {
    // block_hash: String,
    // block_height: u64,
    result: Option<Vec<u8>>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct AccountStateResponse {
    #[serde(with = "dec_format")]
    amount: u128,
    #[serde(with = "dec_format")]
    locked: u128,
    // storage_usage: u64,
    // error: Option<String>,
}

#[derive(Deserialize)]
struct FtMetadata {
    decimals: u8,
}

#[derive(Deserialize)]
struct MTBaseTokenMetadata {
    decimals: u8,
}

#[derive(Debug, Clone)]
pub struct RpcConfig {
    pub rpcs: Vec<String>,
    pub concurrency: usize,
    pub bearer_token: Option<String>,
    pub timeout: Duration,
    pub num_iterations: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcAccountStateResult {
    #[serde(with = "dec_format")]
    #[serde(rename = "b")]
    pub balance: u128,
    #[serde(with = "dec_format")]
    #[serde(rename = "l")]
    pub locked: u128,
    #[serde(rename = "s")]
    pub storage_bytes: u64,
}

impl RpcConfig {
    pub fn from_env() -> Self {
        let rpcs: Vec<_> = env::var("RPCS")
            .expect("Missing env RPCS")
            .split(",")
            .map(|s| s.to_string())
            .collect();
        let num_iterations = env::var("RPC_NUM_ITERATIONS")
            .map(|s| s.parse().unwrap())
            .unwrap_or(rpcs.len());
        let config = RpcConfig {
            rpcs,
            concurrency: env::var("RPC_CONCURRENCY")
                .unwrap_or("100".to_string())
                .parse()
                .unwrap(),
            bearer_token: env::var("RPC_BEARER_TOKEN").ok(),
            timeout: env::var("RPC_TIMEOUT")
                .map(|s| Duration::from_millis(s.parse().unwrap()))
                .unwrap_or(RPC_TIMEOUT),
            num_iterations,
        };
        assert!(config.concurrency > 0);
        assert!(config.rpcs.len() > 0);
        config
    }
}

pub async fn fetch_from_rpc(
    client: &Client,
    tasks: &[Task],
    rpc_config: &RpcConfig,
) -> Result<Vec<TaskResult>, RpcError> {
    if tasks.is_empty() {
        return Ok(vec![]);
    }
    let mut results: Vec<(TaskId, TaskResult)> = Vec::new();
    let start = std::time::Instant::now();
    let (tx, mut rx) =
        mpsc::channel::<Result<(TaskId, TaskResult), RpcError>>(rpc_config.concurrency);
    let rpcs = &rpc_config.rpcs;
    tracing::info!(target: TARGET_RPC, "Fetching {} tasks from RPC", tasks.len());

    for (i, task) in tasks.iter().enumerate() {
        let task_id = TaskId(i);
        let client = client.clone();
        let tx = tx.clone();
        let rpcs = rpcs.clone();
        let task = task.clone();
        let bearer_token = rpc_config.bearer_token.clone();
        let timeout = rpc_config.timeout;
        let num_iterations = rpc_config.num_iterations;

        // Spawn a new asynchronous task for each request
        task::spawn(async move {
            let mut index = i;
            let mut iterations = num_iterations;
            let mut sleep = Duration::from_millis(100);
            let res = loop {
                let url = &rpcs[index % rpcs.len()];
                index += 1;
                let res = execute_task(&client, &url, &task, &bearer_token, timeout).await;

                match res {
                    Ok(result) => {
                        break Ok((task_id, result));
                    }
                    Err(e) => {
                        if !matches!(e, RpcError::RetriableRpcError(_)) {
                            tracing::warn!(target: TARGET_RPC, "RPC Error: {:?}", e);
                        }
                        // Need to retry this task
                        iterations -= 1;
                        if iterations == 0 {
                            break Err(e);
                        }
                        tokio::time::sleep(sleep).await;
                        sleep *= 2;
                    }
                }
            };
            tx.send(res).await.expect("Failed to send task result");
        });
    }

    // Close the sender to ensure the loop below exits once all tasks are completed
    drop(tx);

    let mut errors = Vec::new();
    // Wait for all tasks to complete
    while let Some(res) = rx.recv().await {
        match res {
            Ok(pair) => results.push(pair),
            Err(e) => {
                errors.push(e);
            }
        }
    }
    let duration = start.elapsed().as_millis();

    tracing::debug!(target: TARGET_RPC, "Query {}ms: fetch_from_rpc {} tasks",
        duration,
        tasks.len());

    if let Some(err) = errors.pop() {
        return Err(err);
    }

    results.sort_by_key(|(task_id, _)| *task_id);
    let results: Vec<TaskResult> = results.into_iter().map(|(_, result)| result).collect();

    Ok(results)
}

async fn execute_task(
    client: &Client,
    url: &String,
    task: &Task,
    bearer_token: &Option<String>,
    timeout: Duration,
) -> Result<TaskResult, RpcError> {
    match task {
        Task::AccountBalance {
            account_id,
            block_hash,
        } => {
            let value = rpc_json_request(
                json!({
                    "request_type": "view_account",
                    "account_id": account_id,
                }),
                client,
                url,
                Some(block_hash),
                bearer_token,
                timeout,
            )
            .await?;
            match value {
                Some(value) => parse_account_state(value),
                None => Ok(None),
            }
        }
        Task::FtBalance {
            contract_id,
            account_id,
            block_hash,
        } => {
            let value = rpc_json_request(
                json!({
                    "request_type": "call_function",
                    "account_id": contract_id,
                    "method_name": "ft_balance_of",
                    "args_base64": BASE64_STANDARD.encode(format!("{{\"account_id\": \"{}\"}}", account_id)),
                }),
                client,
                url,
                Some(block_hash),
                bearer_token,
                timeout,
            ).await?;
            match value {
                Some(value) => parse_mt_or_ft_balance(value),
                None => Ok(None),
            }
        }
        Task::FtDecimals {
            contract_id,
            block_hash,
        } => {
            let value = rpc_json_request(
                json!({
                    "request_type": "call_function",
                    "account_id": contract_id,
                    "method_name": "ft_metadata",
                    "args_base64": BASE64_STANDARD.encode("{}"),
                }),
                client,
                url,
                Some(block_hash),
                bearer_token,
                timeout,
            )
            .await?;
            match value {
                Some(value) => parse_ft_decimals(value),
                None => Ok(None),
            }
        }
        Task::MtBalance {
            contract_id,
            token_id,
            account_id,
            block_hash,
        } => {
            let value = rpc_json_request(
                json!({
                    "request_type": "call_function",
                    "account_id": contract_id,
                    "method_name": "mt_balance_of",
                    "args_base64": BASE64_STANDARD.encode(json!({"account_id": account_id, "token_id": token_id}).to_string()),
                }),
                client,
                url,
                Some(block_hash),
                bearer_token,
                timeout,
            ).await?;
            match value {
                Some(value) => parse_mt_or_ft_balance(value),
                None => Ok(None),
            }
        }
        Task::MtDecimals {
            contract_id,
            token_id,
            block_hash,
        } => {
            let value = rpc_json_request(
                json!({
                    "request_type": "call_function",
                    "account_id": contract_id,
                    "method_name": "mt_metadata_base_by_token_id",
                    "args_base64": BASE64_STANDARD.encode(json!({"token_ids": &[token_id]}).to_string()),
                }),
                client,
                url,
                Some(block_hash),
                bearer_token,
                timeout,
            )
            .await?;
            match value {
                Some(value) => parse_mt_decimals(value),
                None => Ok(None),
            }
        }
        Task::CustomViewCall {
            contract_id,
            method_name,
            arguments,
            block_hash,
        } => {
            let value = rpc_json_request(
                json!({
                    "request_type": "call_function",
                    "account_id": contract_id,
                    "method_name": method_name,
                    "args_base64": BASE64_STANDARD.encode(arguments),
                }),
                client,
                url,
                Some(block_hash),
                bearer_token,
                timeout,
            )
            .await?;
            match value {
                Some(value) => parse_custom_view_call(value),
                None => Ok(None),
            }
        }
    }
}

pub async fn fetch_intents_prices_internal(
    client: &Client,
    rpc_config: &RpcConfig,
) -> Result<Vec<IntentsTokenResponse>, RpcError> {
    let response = client
        .get(INTENTS_TOKENS_URL)
        .timeout(rpc_config.timeout)
        .send()
        .await?;
    Ok(response
        .error_for_status()?
        .json::<Vec<IntentsTokenResponse>>()
        .await?)
}

pub async fn fetch_intents_prices(
    client: &Client,
    rpc_config: &RpcConfig,
    ignore_errors: bool,
) -> Result<Option<Vec<IntentsTokenResponse>>, RpcError> {
    // let current_time_ns = std::time::SystemTime::now()
    //     .duration_since(std::time::UNIX_EPOCH)
    //     .unwrap()
    //     .as_nanos() as u64;
    // let time_diff_ns = current_time_ns.saturating_sub(block_timestamp);
    // if time_diff_ns > rpc_config.max_price_latency_ns {
    //     return Ok(None);
    // }
    match fetch_intents_prices_internal(client, rpc_config).await {
        Ok(res) => Ok(Some(res)),
        Err(e) => {
            if ignore_errors {
                Ok(None)
            } else {
                Err(e)
            }
        }
    }
}

async fn rpc_json_request(
    mut params: Value,
    client: &Client,
    url: &String,
    block_hash: Option<&CryptoHash>,
    bearer_token: &Option<String>,
    timeout: Duration,
) -> Result<Option<Value>, RpcError> {
    if let Some(block_hash) = block_hash {
        params["block_id"] = json!(block_hash);
    } else {
        params["finality"] = json!("final");
    }
    let request = JsonRequest {
        jsonrpc: "2.0".to_string(),
        method: "query".to_string(),
        params,
        id: "0".to_string(),
    };
    let mut response = client.post(url);
    if let Some(bearer) = bearer_token {
        response = response.bearer_auth(bearer);
    }
    let response = response.json(&request).timeout(timeout).send().await?;
    let response = response.json::<JsonResponse>().await?;
    if let Some(error) = response.error {
        if let Some(cause) = &error.cause {
            if cause.name == Some(RPC_ERROR_UNKNOWN_BLOCK.to_string())
                || cause.name == Some(RPC_ERROR_UNAVAILABLE_SHARD.to_string())
            {
                return Err(RpcError::RetriableRpcError(cause.name.clone().unwrap()));
            }
        }
        tracing::debug!(target: TARGET_RPC, "RPC Error: {:?}", error);
    }

    Ok(response.result)
}

fn parse_account_state(result: Value) -> Result<TaskResult, RpcError> {
    let account_state: AccountStateResponse =
        serde_json::from_value(result).map_err(|e| RpcError::InvalidAccountStateResponse(e))?;
    Ok(Some(
        serde_json::to_value(U128(account_state.amount + account_state.locked)).unwrap(),
    ))
}

fn parse_mt_or_ft_balance(result: Value) -> Result<TaskResult, RpcError> {
    let fc: FunctionCallResponse =
        serde_json::from_value(result).map_err(|e| RpcError::InvalidFunctionCallResponse(e))?;
    if let Some(error) = fc.error {
        tracing::debug!(target: TARGET_RPC, "FCR Error: {}", error);
    }
    Ok(fc.result.and_then(|result| {
        let balance: Option<U128> = serde_json::from_slice(&result).ok();
        balance.map(|b| serde_json::to_value(b).unwrap())
    }))
}

fn parse_ft_decimals(result: Value) -> Result<TaskResult, RpcError> {
    let fc: FunctionCallResponse =
        serde_json::from_value(result).map_err(|e| RpcError::InvalidFunctionCallResponse(e))?;
    if let Some(error) = fc.error {
        tracing::debug!(target: TARGET_RPC, "FCR Error: {}", error);
    }
    Ok(fc.result.and_then(|result| {
        let metadata: Option<FtMetadata> = serde_json::from_slice(&result).ok();
        metadata.map(|b| serde_json::to_value(b.decimals).unwrap())
    }))
}

fn parse_mt_decimals(result: Value) -> Result<TaskResult, RpcError> {
    let fc: FunctionCallResponse =
        serde_json::from_value(result).map_err(|e| RpcError::InvalidFunctionCallResponse(e))?;
    if let Some(error) = fc.error {
        tracing::debug!(target: TARGET_RPC, "FCR Error: {}", error);
    }
    Ok(fc.result.and_then(|result| {
        let metadata: Option<Vec<MTBaseTokenMetadata>> = serde_json::from_slice(&result).ok();
        metadata.and_then(|b| b.get(0).map(|b| serde_json::to_value(b.decimals).unwrap()))
    }))
}

fn parse_custom_view_call(result: Value) -> Result<TaskResult, RpcError> {
    let fc: FunctionCallResponse =
        serde_json::from_value(result).map_err(|e| RpcError::InvalidFunctionCallResponse(e))?;
    if let Some(error) = fc.error {
        tracing::debug!(target: TARGET_RPC, "FCR Error: {}", error);
    }
    Ok(fc
        .result
        .and_then(|result| serde_json::from_slice(&result).ok()))
}
