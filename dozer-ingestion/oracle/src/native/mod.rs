mod config;
mod server;
use config::get_config_json;
use dozer_ingestion_connector::dozer_types::event::Event;
use dozer_ingestion_connector::dozer_types::grpc_types::oracle::{
    RedoRequest, RedoResponse, RequestCode,
};
use dozer_ingestion_connector::dozer_types::node::{NodeHandle, SourceState};
use dozer_ingestion_connector::dozer_types::tonic;
use dozer_ingestion_connector::tokio;
use dozer_ingestion_connector::tokio::sync::broadcast::Receiver;
use dozer_ingestion_connector::{
    dozer_types::{
        models::ingestion_types::{OracleConfig, OracleNativeReaderOptions},
        types::Schema,
    },
    Ingestor, TableInfo,
};
use std::io::Write;
use std::process::Command;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use tempdir::TempDir;

use crate::connector::Scn;

const REPLICATOR_CMD: &str = "OpenLogReplicator";

pub async fn replicate(
    ingestor: &Ingestor,
    tables: Vec<TableInfo>,
    _schemas: Vec<Schema>,
    checkpoint: Scn,
    config: OracleConfig,
    opts: OracleNativeReaderOptions,
    event_receiver: Receiver<Event>,
    node_handle: NodeHandle,
) -> Result<(), std::io::Error> {
    let tmp_dir = TempDir::new("dozer-native-log")?;
    let config_path = tmp_dir.path().join("OpenLogReplicator.json");

    let json = get_config_json(&config, &tables, &opts, checkpoint);
    let mut tmp_file = std::fs::File::create(config_path.clone())?;
    writeln!(tmp_file, "{0}", json)?;

    // Start replicator as a different process
    let _t = tokio::task::spawn_blocking(move || {
        Command::new(REPLICATOR_CMD)
            .args(["-f", config_path.to_str().unwrap()])
            .output()
            .expect("Failed to run 'OpenLogReplicator'. Do you have this installed on your PATH?")
    });

    // Start server to listen for messages
    server::serve(ingestor, tables, opts, event_receiver, node_handle)
        .await
        .unwrap();

    Ok(())
}

pub fn get_operation_id_from_event(event: &Event, node_handle: &NodeHandle) -> Option<u64> {
    match event {
        Event::SinkFlushed { epoch, .. } => epoch
            .common_info
            .source_states
            .get(node_handle)
            .and_then(|state| match state {
                SourceState::Restartable(id) => Some(id.seq_in_tx),
                _ => None,
            }),
    }
}

pub async fn handle_redo_request(ingestor: &Ingestor, req: RedoRequest) {
    match req.code() {
        RequestCode::Info => todo!(),
        RequestCode::Start => todo!(),
        RequestCode::Continue => todo!(),
        RequestCode::Confirm => todo!(),
    }
}

pub fn map_redo_response(
    res: Result<Event, BroadcastStreamRecvError>,
    node_handle: NodeHandle,
) -> Result<RedoResponse, tonic::Status> {
    todo!()
}
