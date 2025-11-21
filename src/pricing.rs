use crate::rpc::RpcConfig;
use crate::*;
use reqwest::Client;
use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

const PROJECT_ID: &str = "pricing-fetcher";
const MAX_HISTORY_LEN: usize = 2 * 60 * 60; // About 1 hour of data at 500ms intervals

pub type PriceHistorySingleton = Arc<RwLock<PriceHistory>>;
pub type Prices = HashMap<String, f64>;

#[derive(Serialize, Deserialize)]
pub struct PriceHistory {
    pub history: BTreeMap<u64, Prices>,
}

impl PriceHistory {
    pub fn get(&self, timestamp: u64) -> Prices {
        // Return the latest prices before or at the given timestamp
        self.history
            .range(..=timestamp)
            .next_back()
            .map(|(_, prices)| prices.clone())
            .unwrap_or_default()
    }

    pub fn save_to_file(&self, path: &str) -> std::io::Result<()> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, self)?;
        Ok(())
    }

    pub fn load_from_file(path: &str) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let history = serde_json::from_reader(reader)?;
        Ok(history)
    }
}

pub fn start_fetcher(
    is_running: Arc<AtomicBool>,
    client: Client,
    rpc_config: RpcConfig,
) -> PriceHistorySingleton {
    let price_history =
        PriceHistory::load_from_file("res/price_history.json").unwrap_or_else(|e| {
            tracing::warn!(target: PROJECT_ID, "Failed to load price history: {}", e);
            PriceHistory {
                history: BTreeMap::new(),
            }
        });
    let price_history_singleton = Arc::new(RwLock::new(price_history));
    let res = price_history_singleton.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));
        while is_running.load(std::sync::atomic::Ordering::SeqCst) {
            interval.tick().await;
            let intents_tokens = rpc::fetch_intents_prices(&client, &rpc_config, true)
                .await
                .expect("ignoring errors");
            if let Some(intents_tokens) = intents_tokens {
                let prices: HashMap<String, f64> = intents_tokens
                    .into_iter()
                    .map(|token| (token.asset_id, token.price))
                    .collect();
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                {
                    let mut price_history = price_history_singleton.write().unwrap();
                    price_history.history.insert(timestamp, prices);
                    if price_history.history.len() > MAX_HISTORY_LEN {
                        let first_key = *price_history.history.keys().next().unwrap();
                        price_history.history.remove(&first_key);
                    }
                    tracing::log::debug!(target: PROJECT_ID, "Fetched prices at timestamp {}", timestamp);
                }
            } else {
                tracing::log::warn!(target: PROJECT_ID, "Failed to fetch intents prices");
            }
        }
    });
    res
}
