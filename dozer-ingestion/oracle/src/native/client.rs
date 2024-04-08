use std::net::SocketAddr;

use dozer_ingestion_connector::dozer_types::grpc_types::oracle::open_log_replicator_client::OpenLogReplicatorClient;
use dozer_ingestion_connector::dozer_types::grpc_types::oracle::{RedoRequest, RedoResponse};
use dozer_ingestion_connector::dozer_types::log::warn;
use dozer_ingestion_connector::dozer_types::models::ingestion_types::OracleNativeReaderOptions;
use dozer_ingestion_connector::dozer_types::node::NodeHandle;
use dozer_ingestion_connector::dozer_types::tonic;
use dozer_ingestion_connector::futures::StreamExt;
use dozer_ingestion_connector::tokio;
use dozer_ingestion_connector::tokio::sync::broadcast::Receiver;
use dozer_ingestion_connector::{
    dozer_types,
    dozer_types::{log::info, tonic::transport::Server, tracing::Level},
    Ingestor, TableInfo,
};
use tokio_stream::wrappers::BroadcastStream;
use tower_http::trace::{self, TraceLayer};

use dozer_ingestion_connector::dozer_types::event::Event;
use dozer_types::grpc_types::oracle::open_log_replicator_server::{
    OpenLogReplicator, OpenLogReplicatorServer,
};

use crate::native::handle_redo_request;

use super::map_redo_response;

struct NativeLogIngestService {
    ingestor: &'static Ingestor,
    tables: Vec<TableInfo>,
    event_receiver: Receiver<Event>,
    node_handle: NodeHandle,
}

impl NativeLogIngestService {}

type ResponseStream =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<RedoResponse, tonic::Status>> + Send>>;
#[tonic::async_trait]
impl OpenLogReplicator for NativeLogIngestService {
    #[doc = r" Server streaming response type for the Redo method."]
    type RedoStream = ResponseStream;

    async fn redo(
        &self,
        request: tonic::Request<tonic::Streaming<RedoRequest>>,
    ) -> Result<tonic::Response<Self::RedoStream>, tonic::Status> {
        let mut in_stream = request.into_inner();

        let ingestor = self.ingestor;
        let node_handle = self.node_handle.clone();
        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(req) => handle_redo_request(ingestor, req).await,
                    Err(err) => {
                        if let Some(io_err) = match_for_io_error(&err) {
                            if io_err.kind() == std::io::ErrorKind::BrokenPipe {
                                // here you can handle special case when client
                                // disconnected in unexpected way
                                eprintln!("\tclient disconnected: broken pipe");
                                break;
                            }
                        }
                        warn!("NativeLogIngestService: Error in reader server : {err}");
                    }
                }
            }
            info!("\t Oracle native log reader stream ended");
        });

        let out_stream: Self::RedoStream = Box::pin(
            BroadcastStream::new(self.event_receiver.resubscribe()).map(move |evt| {
                let node_handle = node_handle.clone();
                map_redo_response(evt, node_handle)
            }),
        );
        Ok(tonic::Response::new(out_stream))
    }
}

fn match_for_io_error(err_status: &tonic::Status) -> Option<&std::io::Error> {
    let mut err: &(dyn std::error::Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}
