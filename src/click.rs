use clickhouse::{Client, Row};
use serde::Serialize;
use std::env;

use fastnear_primitives::near_primitives::types::BlockHeight;
use serde::de::DeserializeOwned;
use std::time::Duration;

pub const CLICKHOUSE_TARGET: &str = "clickhouse";
pub const SAVE_STEP: u64 = 1000;
pub const MAX_COMMIT_HANDLERS: usize = 3;

#[derive(Clone)]
pub struct ClickDB {
    pub client: Client,
    pub prefix: String,
    pub min_batch: usize,
}

impl ClickDB {
    pub fn new(min_batch: usize, prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            client: establish_connection(prefix),
            min_batch,
        }
    }

    pub async fn max_in_range(
        &self,
        column: &str,
        table: &str,
        start_block: BlockHeight,
        end_block: BlockHeight,
    ) -> clickhouse::error::Result<BlockHeight> {
        let block_height = self
            .client
            .query(&format!(
                "SELECT max({column}) FROM {table} where block_height >= {start_block} and block_height < {end_block}"
            ))
            .fetch_one::<u64>()
            .await?;
        Ok(block_height)
    }

    pub async fn verify_connection(&self) -> clickhouse::error::Result<()> {
        self.client.query("SELECT 1").execute().await?;
        Ok(())
    }

    pub async fn read_rows<T>(&self, query: &str) -> clickhouse::error::Result<Vec<T>>
    where
        T: Row + DeserializeOwned,
    {
        let rows = self.client.query(query).fetch_all::<T>().await?;
        Ok(rows)
    }
}

fn establish_connection(prefix: &str) -> Client {
    Client::default()
        .with_url(env::var(format!("{prefix}DATABASE_URL")).unwrap())
        .with_user(env::var(format!("{prefix}DATABASE_USER")).unwrap())
        .with_password(env::var(format!("{prefix}DATABASE_PASSWORD")).unwrap())
        .with_database(env::var(format!("{prefix}DATABASE_DATABASE")).unwrap())
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
