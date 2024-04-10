use std::sync::{mpsc, Arc, Mutex};
use std::{sync::mpsc::SyncSender, time::Duration};

use dozer_ingestion_connector::dozer_types::chrono::{DateTime, Utc};
use dozer_ingestion_connector::dozer_types::models::ingestion_types::LogMinerConfig;
use dozer_ingestion_connector::Ingestor;

use oracle::{sql_type::FromSql, Connection, RowValue};

use crate::connector::ConnectConfig;
use crate::connector::{
    replicate::log::{
        listing::LogCollector,
        redo::{add_logfiles, LogMinerSession},
    },
    Result, Scn,
};

mod listing;
mod redo;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TransactionId([u8; 8]);

impl FromSql for TransactionId {
    fn from_sql(val: &oracle::SqlValue) -> oracle::Result<Self>
    where
        Self: Sized,
    {
        let v: Vec<u8> = val.get()?;
        Ok(Self(v.try_into().unwrap()))
    }
}

const SCN_GAP_MIN_SIZE: Scn = 1_000_000;

const OP_CODE_INSERT: u8 = 1;
const OP_CODE_DELETE: u8 = 2;
const OP_CODE_UPDATE: u8 = 3;
const OP_CODE_DDL: u8 = 5;
const OP_CODE_COMMIT: u8 = 7;
const OP_CODE_MISSING_SCN: u8 = 34;
const OP_CODE_ROLLBACK: u8 = 36;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum OperationType {
    Insert = OP_CODE_INSERT,
    Delete = OP_CODE_DELETE,
    Update = OP_CODE_UPDATE,
    Ddl = OP_CODE_DDL,
    Commit = OP_CODE_COMMIT,
    Rollback = OP_CODE_ROLLBACK,
    MissingScn = OP_CODE_MISSING_SCN,
    Unsupported,
}

impl FromSql for OperationType {
    fn from_sql(val: &oracle::SqlValue) -> oracle::Result<Self>
    where
        Self: Sized,
    {
        let v: u8 = val.get()?;
        Ok(match v {
            OP_CODE_INSERT => Self::Insert,
            OP_CODE_DELETE => Self::Delete,
            OP_CODE_UPDATE => Self::Update,
            OP_CODE_DDL => Self::Ddl,
            OP_CODE_COMMIT => Self::Commit,
            OP_CODE_MISSING_SCN => Self::MissingScn,
            OP_CODE_ROLLBACK => Self::Rollback,
            _ => Self::Unsupported,
        })
    }
}

#[derive(Debug, Clone, RowValue)]
/// This is a raw row from V$LOGMNR_CONTENTS
pub struct LogMinerContent {
    pub scn: Scn,
    pub timestamp: DateTime<Utc>,
    pub xid: TransactionId,
    pub pxid: TransactionId,
    #[row_value(rename = "operation_code")]
    pub operation_type: OperationType,
    pub seg_owner: Option<String>,
    pub table_name: Option<String>,
    pub sql_redo: Option<String>,
    pub csf: u8,
}

/// `ingestor` is only used for checking if ingestion has ended so we can break the loop.
pub fn log_miner_loop(
    connect_config: ConnectConfig,
    connection: Arc<Connection>,
    start_scn: Scn,
    con_id: Option<u32>,
    config: LogMinerConfig,
    sender: SyncSender<LogMinerContent>,
    ingestor: &Ingestor,
) -> Result<()> {
    log_reader_loop(
        connect_config,
        connection,
        start_scn,
        con_id,
        config,
        sender,
        ingestor,
    )
}

macro_rules! ora_try {
    ($ingestor:expr, $expr:expr, $msg:literal $(,$param:expr)*) => {{
        match $expr {
            Ok(v) => v,
            Err(e) => {
                let e: crate::connector::Error = e.into();
            if $ingestor.is_closed() {
                return Ok(());
            }
            dozer_ingestion_connector::dozer_types::log::error!($msg, e, $($param),*);
            continue;
        }
        }
    }};
}

struct LogminerTask {
    start_scn: Scn,
    end_scn: Scn,
    result_sender: crossbeam_channel::Sender<Result<LogMinerContent>>,
}

fn do_log_mining(
    connection: &Connection,
    con_id: Option<u32>,
    fetch_batch_size: u32,
    start_scn: Scn,
    end_scn: Scn,
) -> Result<impl Iterator<Item = std::result::Result<LogMinerContent, oracle::Error>>> {
    let mining_session = LogMinerSession::start(connection, start_scn, end_scn, fetch_batch_size)?;

    let stmt = mining_session.stmt(con_id)?;
    let results: oracle::ResultSet<LogMinerContent> = stmt.into_result_set(&[])?;
    Ok(results)
}

fn log_reader_loop(
    connect_config: ConnectConfig,
    connection: Arc<Connection>,
    mut start_scn: Scn,
    con_id: Option<u32>,
    config: LogMinerConfig,
    sender: SyncSender<LogMinerContent>,
    ingestor: &Ingestor,
) -> Result<()> {
    let log_collector = LogCollector::new(connection.clone());
    let mut logs = log_collector.get_logs(start_scn)?;
    add_logfiles(connection.as_ref(), &logs)?;
    // let logminer_session_mutex = Arc::new(Mutex::new(0));
    let (work_sender, work_receiver) = crossbeam_channel::bounded::<LogminerTask>(100);
    for _ in 0..4 {
        let work_receiver = work_receiver.clone();
        // let logminer_session_mutex = logminer_session_mutex.clone();
        let connect_config = connect_config.clone();
        std::thread::spawn(move || {
            'outer: for LogminerTask {
                start_scn,
                end_scn,
                result_sender,
            } in work_receiver.clone()
            {
                let connection = Connection::connect(
                    &connect_config.username,
                    connect_config.password.clone(),
                    connect_config.connect_string.clone(),
                )
                .unwrap();
                let rows = {
                    // let _guard = logminer_session_mutex.lock();
                    let rows = do_log_mining(
                        &connection,
                        con_id,
                        config.fetch_batch_size,
                        start_scn,
                        end_scn,
                    );
                    rows
                };
                match rows {
                    Ok(rows) => {
                        for row in rows {
                            if result_sender.send(row.map_err(Into::into)).is_err() {
                                continue 'outer;
                            }
                        }
                    }
                    Err(e) => {
                        if result_sender.send(Err(e)).is_err() {
                            continue 'outer;
                        }
                    }
                }
            }
        });
    }

    loop {
        if ingestor.is_closed() {
            break;
        }
        let cur_scn: Scn = ora_try!(
            ingestor,
            connection.query_row_as("SELECT current_scn FROM V$DATABASE", &[]),
            "Error getting current scn: {0}"
        );

        let end_scn = if cur_scn - start_scn > SCN_GAP_MIN_SIZE {
            cur_scn
        } else {
            start_scn + config.scn_batch_size
        };

        let part_size = (end_scn - start_scn) / 4;
        let mut result_receivers = vec![];
        for i in 0..4 {
            let start = start_scn + i * part_size;
            let (send, recv) = crossbeam_channel::bounded(part_size as usize * 4);
            work_sender
                .send(LogminerTask {
                    start_scn: start,
                    end_scn: start + part_size,
                    result_sender: send,
                })
                .unwrap();
            result_receivers.push(recv);
        }

        for results in result_receivers {
            for result in results {
                let r = ora_try!(ingestor, result, "error reading log entry: {}");
                if r.operation_type != OperationType::MissingScn {
                    start_scn = r.scn;
                }

                let Ok(_) = sender.send(r) else {
                    return Ok(());
                };
            }
        }
        std::thread::sleep(Duration::from_millis(config.poll_interval_in_milliseconds));

        let new_logs = ora_try!(
            ingestor,
            log_collector.get_logs(start_scn),
            "Error listing logs: {0}"
        );

        if new_logs != logs {
            // We end the session here to do some clean up to avoid very
            // long-running logminer sessions, which might leak resources.
            LogMinerSession::end(connection.as_ref());
            loop {
                ora_try!(
                    ingestor,
                    add_logfiles(connection.as_ref(), &new_logs),
                    "Error adding log files: {}"
                );
                break;
            }
            logs = new_logs;
        };
    }
    Ok(())
}
