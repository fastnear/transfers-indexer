mod click;
mod common;
mod transfers;

use crate::click::*;
use std::sync::Arc;

use dotenv::dotenv;
use fastnear_neardata_fetcher::fetcher;
use fastnear_primitives::block_with_tx_hash::*;
use fastnear_primitives::types::ChainId;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

const PROJECT_ID: &str = "transfer-indexer";

const SAFE_CATCH_UP_OFFSET: u64 = 1000;

#[tokio::main]
async fn main() {
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

    common::setup_tracing("clickhouse=info,transfer-indexer=info,neardata-fetcher=info");

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
    let backfill_block_height = args
        .get(2)
        .map(|v| v.parse().expect("Failed to parse backfill block height"));

    match command {
        "actions" => {
            let mut actions_data = ActionsData::new();
            let db_last_block_height = actions_data.last_block_height(&db).await;
            let last_block_height = backfill_block_height.unwrap_or(db_last_block_height);
            let start_block_height = first_block_height.max(last_block_height + 1);
            let (sender, receiver) = mpsc::channel(100);
            let mut builder = fetcher::FetcherConfigBuilder::new()
                .chain_id(chain_id)
                .num_threads(num_threads)
                .start_block_height(start_block_height);
            if let Some(auth_bearer_token) = auth_bearer_token {
                builder = builder.auth_bearer_token(auth_bearer_token);
            }
            tokio::spawn(fetcher::start_fetcher(builder.build(), sender, is_running));
            listen_blocks_for_actions(receiver, db, actions_data, last_block_height).await;
        }
        "transactions" => {
            let mut transactions_data = TransactionsData::new();
            let db_last_block_height = transactions_data.last_block_height(&db).await;
            let last_block_height = backfill_block_height.unwrap_or(db_last_block_height);
            let is_cache_ready = transactions_data.is_cache_ready(last_block_height);
            tracing::log::info!(target: PROJECT_ID, "Last block height: {}. Cache is ready: {}", last_block_height, is_cache_ready);

            let start_block_height = if is_cache_ready {
                last_block_height + 1
            } else {
                last_block_height.saturating_sub(SAFE_CATCH_UP_OFFSET)
            };

            let start_block_height = first_block_height.max(start_block_height);
            let (sender, receiver) = mpsc::channel(100);
            let mut builder = fetcher::FetcherConfigBuilder::new()
                .chain_id(chain_id)
                .num_threads(num_threads)
                .start_block_height(start_block_height);
            if let Some(auth_bearer_token) = auth_bearer_token {
                builder = builder.auth_bearer_token(auth_bearer_token);
            }
            tokio::spawn(fetcher::start_fetcher(builder.build(), sender, is_running));
            listen_blocks_for_transactions(receiver, db, transactions_data, last_block_height)
                .await;
        }
        _ => {
            panic!("Unknown command");
        }
    };

    tracing::log::info!(target: PROJECT_ID, "Gracefully shut down");
}


async fn listen_blocks_for_transactions(
    mut stream: mpsc::Receiver<BlockWithTxHashes>,
    db: ClickDB,
    mut transactions_data: TransactionsData,
    last_block_height: u64,
) {
    let mut prev_block_hash = None;
    while let Some(block) = stream.recv().await {
        let block_height = block.block.header.height;
        tracing::log::info!(target: PROJECT_ID, "Processing block: {}", block_height);
        prev_block_hash = Some(
            transactions_data
                .process_block(&db, block, last_block_height, prev_block_hash)
                .await
                .unwrap(),
        );
    }
    tracing::log::info!(target: PROJECT_ID, "Committing the last batch");
    transactions_data.commit(&db).await.unwrap();
    transactions_data.flush().await.unwrap();
}
