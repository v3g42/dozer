use dozer_ingestion_connector::dozer_types::grpc_types::oracle::{RedoRequest, RedoResponse};
use dozer_ingestion_connector::dozer_types::models::ingestion_types::OracleNativeReaderOptions;
use dozer_ingestion_connector::dozer_types::tonic;
use dozer_ingestion_connector::tokio;
use dozer_ingestion_connector::tokio::sync::mpsc::{self, Receiver, Sender};
use dozer_ingestion_connector::{
    dozer_types,
    dozer_types::{log::info, tonic::transport::Server, tracing::Level},
    Ingestor, TableInfo,
};
use tokio_stream::StreamExt;
use tower_http::trace::{self, TraceLayer};

use dozer_types::grpc_types::oracle::open_log_replicator_server::{
    OpenLogReplicator, OpenLogReplicatorServer,
};

pub struct OracleNativeLogServer {
    pub config: OracleNativeReaderOptions,
}
impl OracleNativeLogServer {
    pub async fn serve(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let addr = self.config.uri.parse()?;

        // Ingestor will live as long as the server
        // Refactor to use Arc
        let ingestor = unsafe { std::mem::transmute::<&'_ Ingestor, &'static Ingestor>(ingestor) };

        let ingest_service = NativeLogIngestService { ingestor, tables };
        let ingest_service = tonic_web::enable(OpenLogReplicatorServer::new(ingest_service));

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(
                dozer_types::grpc_types::ingest::FILE_DESCRIPTOR_SET,
            )
            .build()
            .unwrap();
        info!(
            "Starting Native Oracle Log  GRPC Server  on {}",
            self.config.uri,
        );
        Server::builder()
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
            )
            .accept_http1(true)
            .add_service(ingest_service)
            .add_service(reflection_service)
            .serve(addr)
            .await
            .map_err(Into::into)
    }
}

struct NativeLogIngestService {
    ingestor: &'static Ingestor,
    tables: Vec<TableInfo>,
    tx: Sender<RedoRequest>,
    rx: Receiver<RedoResponse>,
}

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
        // let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(v) => self
                        .tx
                        .send(v)
                        .await
                        .expect("Should be able to send to Sender<RedoRequest>"),
                    Err(err) => {
                        if let Some(io_err) = match_for_io_error(&err) {
                            if io_err.kind() == ErrorKind::BrokenPipe {
                                // here you can handle special case when client
                                // disconnected in unexpected way
                                eprintln!("\tclient disconnected: broken pipe");
                                break;
                            }
                        }

                        match tx.send(Err(err)).await {
                            Ok(_) => (),
                            Err(_err) => break, // response was droped
                        }
                    }
                }
            }
            println!("\tstream ended");
        });
        todo!()
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
