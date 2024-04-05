use indicatif::{ProgressBar, ProgressStyle};
use oracle::{Connection, Error};
use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::time::Duration;

#[derive(Debug, Deserialize, Clone)]
struct StressConfig {
    connection_string: String,
    user: String,
    password: String,
    sql: String,
    bootstrap: String,
    parallel: usize,
    timeout_millis: u64,
    total_per_task: usize,
    record_size: u64,
}

pub fn get_progress() -> ProgressBar {
    let pb = ProgressBar::new_spinner();

    pb.set_style(
        ProgressStyle::with_template("{spinner:.blue} {msg}: {pos}: {per_sec}")
            .unwrap()
            // For more spinners check out the cli-spinners project:
            // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
            .tick_strings(&[
                "▹▹▹▹▹",
                "▸▹▹▹▹",
                "▹▸▹▹▹",
                "▹▹▸▹▹",
                "▹▹▹▸▹",
                "▹▹▹▹▸",
                "▪▪▪▪▪",
            ]),
    );
    pb
}

async fn execute_sql(conn: &Connection, config: &StressConfig) -> Result<(), Error> {
    // Execute SQL
    conn.execute(&config.sql, &[])?;
    conn.commit()?;

    Ok(())
}

async fn bootstrap(config: &StressConfig) -> Result<(), Error> {
    let conn = Connection::connect(&config.user, &config.password, &config.connection_string)?;

    // Execute SQL
    conn.execute(&config.bootstrap, &[])?;
    conn.commit()?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read and parse the configuration file
    let mut file = File::open("stress.yaml")?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let config: StressConfig = serde_yaml::from_str(&contents)?;
    let p = get_progress();
    // bootstrap
    bootstrap(&config).await?;

    let mut tasks = vec![];

    for _ in 0..config.parallel {
        let p = p.clone();
        let config_clone = config.clone();
        // Spawn a new async task for executing the SQL
        let task = tokio::spawn(async move {
            let conn = Connection::connect(
                &config_clone.user,
                &config_clone.password,
                &config_clone.connection_string,
            )
            .unwrap();
            for _ in 0..config_clone.total_per_task {
                if let Err(e) = execute_sql(&conn, &config_clone).await {
                    eprintln!("Failed to execute SQL: {}", e);
                }
                p.inc(1 * config.record_size);
                tokio::time::sleep(Duration::from_millis(config.timeout_millis)).await;
            }
        });
        tasks.push(task);
    }

    // Await all tasks to complete
    for task in tasks {
        let _ = task.await;
    }

    Ok(())
}
