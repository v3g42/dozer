use std::io::Write;

use dozer_types::models::{app_config::Config, connection::ConnectionConfig};
use dozer_types::serde_yaml;
use schemars::{
    gen::{SchemaGenerator, SchemaSettings},
    schema::RootSchema,
    schema_for,
};
fn main() {
    // let config = serde_yaml::from_str::<Config>(include_str!("./config.yaml")).unwrap();

    generate_schema(
        &schema_for!(Config),
        "dozer-types/json_schema/config-schema.json",
    );
    // generate(&config, "dozer-types/json_schema/config-schema.json");
    // generate(&config, "dozer-types/json_schema/config-schema.json");
}

fn generate<T>(obj: &T, path: &str)
where
    T: dozer_types::serde::Serialize,
{
    let gen = SchemaGenerator::new(SchemaSettings::draft07().with(|s| {
        s.inline_subschemas = true;
    }));
    let json_schema = gen.into_root_schema_for_value(&obj).unwrap();
    let str = serde_json::to_string_pretty(&json_schema).unwrap();

    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .unwrap();

    f.write(str.as_bytes()).unwrap();
}

fn generate_schema(json_schema: &RootSchema, path: &str) {
    let str = serde_json::to_string_pretty(&json_schema).unwrap();

    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .unwrap();

    f.write(str.as_bytes()).unwrap();
}
