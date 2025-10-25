use clickhouse::{Client, Row};
use std::env;

use serde::Serialize;

use fastnear_primitives::near_primitives::types::BlockHeight;
use std::time::Duration;

pub const CLICKHOUSE_TARGET: &str = "clickhouse";
pub const SAVE_STEP: u64 = 1000;
pub const MAX_COMMIT_HANDLERS: usize = 3;

#[derive(Clone)]
pub struct ClickDB {
    pub client: Client,
    pub min_batch: usize,
}

impl ClickDB {
    pub fn new(min_batch: usize) -> Self {
        Self {
            client: establish_connection(),
            min_batch,
        }
    }

    pub async fn max(&self, column: &str, table: &str) -> clickhouse::error::Result<BlockHeight> {
        let block_height = self
            .client
            .query(&format!("SELECT max({}) FROM {}", column, table))
            .fetch_one::<u64>()
            .await?;
        Ok(block_height)
    }

    pub async fn verify_connection(&self) -> clickhouse::error::Result<()> {
        self.client.query("SELECT 1").execute().await?;
        Ok(())
    }
}

fn establish_connection() -> Client {
    Client::default()
        .with_url(env::var("DATABASE_URL").unwrap())
        .with_user(env::var("DATABASE_USER").unwrap())
        .with_password(env::var("DATABASE_PASSWORD").unwrap())
        .with_database(env::var("DATABASE_DATABASE").unwrap())
}

pub async fn insert_rows_with_retry<T>(
    client: &Client,
    rows: &Vec<T>,
    table: &str,
) -> clickhouse::error::Result<()>
where
    T: Row + Serialize,
{
    let mut delay = Duration::from_millis(100);
    let max_retries = 10;
    let mut i = 0;
    loop {
        let res = || async {
            if env::var("CLICKHOUSE_SKIP_COMMIT") != Ok("true".to_string()) {
                let mut insert = client.insert(table)?;
                for row in rows {
                    insert.write(row).await?;
                }
                insert.end().await?;
            }
            Ok(())
        };
        match res().await {
            Ok(v) => break Ok(v),
            Err(err) => {
                tracing::log::error!(target: CLICKHOUSE_TARGET, "Attempt #{}: Error inserting rows into \"{}\": {}", i, table, err);
                tokio::time::sleep(delay).await;
                delay *= 2;
                if i == max_retries - 1 {
                    break Err(err);
                }
            }
        };
        i += 1;
    }
}
