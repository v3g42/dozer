mod config;
use crate::connector::Scn;
use config::get_config_json;
use dozer_ingestion_connector::dozer_types::event::Event;
use dozer_ingestion_connector::dozer_types::grpc_types::oracle::open_log_replicator_client::OpenLogReplicatorClient;
use dozer_ingestion_connector::dozer_types::grpc_types::oracle::redo_request::TmVal;
use dozer_ingestion_connector::dozer_types::grpc_types::oracle::{
    RedoRequest, RedoResponse, RequestCode,
};
use dozer_ingestion_connector::dozer_types::log::info;
use dozer_ingestion_connector::dozer_types::node::{NodeHandle, SourceState};
use dozer_ingestion_connector::dozer_types::tonic;
use dozer_ingestion_connector::futures::StreamExt;
use dozer_ingestion_connector::tokio;
use dozer_ingestion_connector::tokio::sync::broadcast::Receiver;
use dozer_ingestion_connector::tokio::sync::mpsc;
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
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::ReceiverStream;

const REPLICATOR_CMD: &str = "OpenLogReplicator";

pub struct OracleNativeReplicator {
    tables: Vec<(usize, TableInfo)>,
    schemas: Vec<Schema>,
    checkpoint: Scn,
    config: OracleConfig,
    opts: OracleNativeReaderOptions,
    event_receiver: Receiver<Event>,
    node_handle: NodeHandle,
}
impl OracleNativeReplicator {
    pub fn new(
        tables: Vec<(usize, TableInfo)>,
        schemas: Vec<Schema>,
        checkpoint: Scn,
        config: OracleConfig,
        opts: OracleNativeReaderOptions,
        event_receiver: Receiver<Event>,
        node_handle: NodeHandle,
    ) -> Self {
        Self {
            tables,
            schemas,
            checkpoint,
            config,
            opts,
            event_receiver,
            node_handle,
        }
    }

    pub async fn replicate(&self, ingestor: &Ingestor) -> Result<(), std::io::Error> {
        let tmp_dir = TempDir::new("dozer-native-log")?;
        let config_path = tmp_dir.path().join("OpenLogReplicator.json");

        let json = get_config_json(&self.config, &self.tables, &self.opts, self.checkpoint);
        let mut tmp_file = std::fs::File::create(config_path.clone())?;
        writeln!(tmp_file, "{0}", json)?;

        // Start replicator as a different process
        if self.opts.run_inline {
            let _t = tokio::task::spawn_blocking(move || {
                Command::new(REPLICATOR_CMD)
            .args(["-f", config_path.to_str().unwrap()])
            .output()
            .expect("Failed to run 'OpenLogReplicator'. Do you have this installed on your PATH?")
            });
        }

        // Start server to listen for messages
        self.connect().await.unwrap();

        Ok(())
    }

    pub async fn connect(&self) -> Result<(), Box<dyn std::error::Error>> {
        //  https://github.com/bersler/OpenLogReplicator/blob/master/documentation/user-manual/user-manual.adoc#communication-protocol

        info!("Connecting to OLR: {}", self.opts.uri);
        let mut client = OpenLogReplicatorClient::connect(self.opts.uri.clone()).await?;

        let (tx, rx) = mpsc::channel::<RedoRequest>(128);

        let in_stream = ReceiverStream::new(rx);
        let in_stream = Box::pin(in_stream);
        info!("Requesting redo: {}", self.opts.uri);

        // Send Info Message

        let info_request = self.redo_request(RequestCode::Info);
        info!(
            "OLR: Requesting Info: {}, {:?}",
            self.opts.uri, info_request
        );
        let bytes: &[u8] = unsafe { any_as_u8_slice(&info_request) };
        info!("bytes: {:x?}", bytes);

        tx.send(info_request).await?;

        let response = client.redo(in_stream).await?;
        let mut out_stream = response.into_inner();
        let mut start_request = self.redo_request(RequestCode::Start);
        start_request.tm_val = Some(TmVal::Scn(self.checkpoint));

        tx.send(start_request).await?;

        // Wait for response
        let info_response = out_stream.next().await.expect("response is expected")?;

        let mut start_request = self.redo_request(RequestCode::Start);

        info!("OLR INFO: {:?}", info_response);

        let mut follow_request = self.redo_request(RequestCode::Start);
        start_request.tm_val = Some(TmVal::Scn(self.checkpoint));
        match info_response.code() {
            dozer_ingestion_connector::dozer_types::grpc_types::oracle::ResponseCode::Ready => {
                follow_request.tm_val = Some(TmVal::Scn(self.checkpoint));
                follow_request.code = RequestCode::Start as i32;
            }
            dozer_ingestion_connector::dozer_types::grpc_types::oracle::ResponseCode::Replicate => {
                follow_request.c_scn = Some(self.checkpoint);
                follow_request.c_idx = Some(0);
                follow_request.code = RequestCode::Continue as i32;
            }
            r => panic!("Should not reach here yet {:?}", r),
        }

        // Start
        info!("OLR: Requesting Start: {}", self.opts.uri);

        tx.send(follow_request).await?;

        let _t = tokio::spawn(async move {
            while let Some(msg) = out_stream.next().await {
                println!("{:?}", msg);
            }
        });

        Ok(())
    }

    pub fn redo_request(&self, code: RequestCode) -> RedoRequest {
        RedoRequest {
            code: code as i32,
            database_name: self.config.sid.clone(),
            seq: None,
            schema: vec![],
            c_scn: None,
            c_idx: None,
            tm_val: None,
        }
    }
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

pub fn map_redo_response(
    res: Result<Event, BroadcastStreamRecvError>,
    node_handle: NodeHandle,
) -> Result<RedoResponse, tonic::Status> {
    todo!()
}

unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::core::slice::from_raw_parts((p as *const T) as *const u8, ::core::mem::size_of::<T>())
}
