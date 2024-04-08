use dozer_ingestion_connector::{
    dozer_types::models::ingestion_types::{OracleConfig, OracleNativeReaderOptions},
    TableInfo,
};
use serde_json::{json, Value};

use crate::connector::Scn;

fn merge(a: &mut Value, b: Value) {
    match (a, b) {
        (a @ &mut Value::Object(_), Value::Object(b)) => {
            let a = a.as_object_mut().unwrap();
            for (k, v) in b {
                merge(a.entry(k).or_insert(Value::Null), v);
            }
        }
        (a, b) => *a = b,
    }
}

pub fn get_config_json(
    config: &OracleConfig,
    tables: &[TableInfo],
    log_reader_options: &OracleNativeReaderOptions,
    _checkpoint: Scn,
) -> Value {
    let server = format!("//{0}:{1}/{2}", config.host, config.password, config.sid);

    let mut source = json!({
      "alias": "S1",
      "name": "ORA1",
      "reader": {
        "type": "online",
        "user": config.user,
        "password": config.password,
        "server": server,
      },
      "format": {
        "type": "json"
      },
      "filter": {
        "table": json!(tables.iter().map(|t: &TableInfo| {return json!( {
            "owner": t.schema,
            "table": t.name
          })}).collect::<Vec<Value>>())
      }
    });

    if let Some(override_source) = &log_reader_options.override_source {
        let override_source = serde_json::to_value(override_source).unwrap();
        merge(&mut source, override_source);
    }

    let mut target = json!({
      "alias": "T1",
      "source": "S1",
      "writer": {
        "type": "network",
        "uri": log_reader_options.uri
      }
    });

    if let Some(override_target) = &log_reader_options.override_target {
        let override_target = serde_json::to_value(&override_target).unwrap();
        merge(&mut target, override_target);
    }
    let mut json = json!({
      "version": "1.5.0",
      "source": [source],
      "target": [target]
    });

    if let Some(metrics) = &log_reader_options.metrics {
        let metrics_json = json!({
          "type": "prometheus",
          "bind": metrics.host.to_string(),
          "tag-names": "all"
        });
        merge(&mut json, metrics_json);
    }

    if let Some(override_json) = &log_reader_options.override_json {
        let override_json = serde_json::to_value(&override_json).unwrap();
        merge(&mut json, override_json);
    }
    json
}
