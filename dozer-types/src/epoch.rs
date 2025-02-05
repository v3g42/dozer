use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{
    node::{NodeHandle, OpIdentifier, SourceStates},
    types::Operation,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Epoch {
    pub id: u64,
    pub details: SourceStates,
}

impl Epoch {
    pub fn new(id: u64, details: SourceStates) -> Self {
        Self { id, details }
    }

    pub fn from(id: u64, node_handle: NodeHandle, txid: u64, seq_in_tx: u64) -> Self {
        Self {
            id,
            details: [(node_handle, OpIdentifier::new(txid, seq_in_tx))]
                .into_iter()
                .collect(),
        }
    }
}

impl Display for Epoch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let details_str = self
            .details
            .iter()
            .map(|e| format!("{} -> {}:{}", e.0, e.1.txid, e.1.seq_in_tx))
            .fold(String::new(), |a, b| a + ", " + b.as_str());
        f.write_str(format!("epoch: {}, details: {}", self.id, details_str).as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutorOperation {
    Op { op: Operation },
    Commit { epoch: Epoch },
    Terminate,
    SnapshottingDone {},
}
