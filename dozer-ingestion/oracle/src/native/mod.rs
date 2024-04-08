use crate::connector::native::config::get_config_json;

use super::Scn;
use dozer_ingestion_connector::{
    dozer_types::{
        models::ingestion_types::{OracleConfig, OracleNativeReaderOptions},
        types::Schema,
    },
    Ingestor, TableInfo,
};
use std::io::Write;
use std::process::Command;

use tempdir::TempDir;

const REPLICATOR_CMD: &str = "OpenLogReplicator";
mod config;
mod server;
pub fn native_reader(
    ingestor: &Ingestor,
    tables: Vec<TableInfo>,
    schemas: Vec<Schema>,
    checkpoint: Scn,
    config: &OracleConfig,
    log_reader_options: &OracleNativeReaderOptions,
) -> Result<(), std::io::Error> {
    let tmp_dir = TempDir::new("dozer-native-log")?;
    let config_path = tmp_dir.path().join("OpenLogReplicator.json");

    let json = get_config_json(config, tables, log_reader_options);
    let mut tmp_file = std::fs::File::create(config_path.clone())?;
    writeln!(tmp_file, "{0}", json)?;

    let _t = std::thread::spawn(move || {
        Command::new(REPLICATOR_CMD)
            .args(["-f", config_path.to_str().unwrap()])
            .output()
            .expect("Failed to run 'OpenLogReplicator'. Do you have this installed on your PATH?")
    });

    Ok(())
}
