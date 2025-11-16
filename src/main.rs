mod block_indexer;
mod click;
mod common;
mod pricing;
mod rpc;
mod transfers;
mod types;

use crate::click::*;
use std::sync::Arc;

use crate::pricing::PriceHistorySingleton;
use crate::types::TransferType;
use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::block_with_tx_hash::*;
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

    let db = ClickDB::new(10000);
    db.verify_connection()
        .await
        .expect("Failed to connect to Clickhouse");

    let client = reqwest::Client::new();
    let chain_id = ChainId::try_from(std::env::var("CHAIN_ID").expect("CHAIN_ID is not set"))
        .expect("Invalid chain id");
    let num_threads = std::env::var("NUM_FETCHING_THREADS")
        .expect("NUM_FETCHING_THREADS is not set")
        .parse::<u64>()
        .expect("Invalid NUM_FETCHING_THREADS");
    let auth_bearer_token = std::env::var("AUTH_BEARER_TOKEN").ok();

    let first_block_height = fetcher::fetch_first_block(&client, chain_id)
        .await
        .expect("First block doesn't exists")
        .block
        .header
        .height;

    tracing::log::info!(target: PROJECT_ID, "First block: {}", first_block_height);

    let args: Vec<String> = std::env::args().collect();
    let transfer_types_str = args.get(1).expect("Command is not specified");
    let transfer_types = if transfer_types_str.to_lowercase() == "all" {
        None
    } else {
        Some(
            transfer_types_str
                .split(',')
                .map(|s| {
                    serde_json::from_str::<TransferType>(&format!("\"{s}\""))
                        .map_err(|err| format!("Invalid transfer type `{s}`: {:?}", err))
                        .unwrap()
                })
                .collect::<Vec<_>>(),
        )
    };

    let start_block_height: Option<BlockHeight> = args
        .get(2)
        .map(|v| v.parse().expect("Failed to parse backfill block height"));

    let end_block_height: Option<BlockHeight> = args
        .get(3)
        .map(|v| v.parse().expect("Failed to parse end block height"));

    let mut transfers_indexer = transfers::TransfersIndexer::new(transfer_types.as_deref());
    let db_last_block_height = transfers_indexer
        .last_block_in_range(
            &db,
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

    // Stating intents pricing thread
    if end_block_height.is_none() {
        transfers_indexer.price_history = Some(pricing::start_fetcher(
            is_running.clone(),
            client.clone(),
            transfers_indexer.rpc_config.clone(),
        ));
    }

    let (sender, mut receiver) = mpsc::channel(100);
    let mut builder = fetcher::FetcherConfigBuilder::new()
        .chain_id(chain_id)
        .num_threads(num_threads)
        .start_block_height(start_block_height);
    if let Some(end_block_height) = end_block_height {
        builder = builder.end_block_height(end_block_height.saturating_sub(1));
    }
    if let Some(auth_bearer_token) = auth_bearer_token {
        builder = builder.auth_bearer_token(auth_bearer_token);
    }
    tokio::spawn(fetcher::start_fetcher(
        builder.build(),
        sender,
        is_running.clone(),
    ));

    while let Some(block) = receiver.recv().await {
        if is_running.load(Ordering::SeqCst) {
            let block_height = block.block.header.height;
            let block_timestamp = block.block.header.timestamp;
            let current_time_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let time_diff_ns = current_time_ns.saturating_sub(block_timestamp);
            tracing::log::info!(target: PROJECT_ID, "Processing block {}\tlatency {:.3} sec", block_height, time_diff_ns as f64 / 1e9f64);
            transfers_indexer.process_block(&db, block).await.unwrap()
        }
    }
    tracing::log::info!(target: PROJECT_ID, "Committing the last batch");
    transfers_indexer.commit(&db).await.unwrap();
    transfers_indexer.flush().await.unwrap();

    tracing::log::info!(target: PROJECT_ID, "Gracefully shut down");
}
