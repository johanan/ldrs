use crate::pq::ColumnDefintion;
use anyhow::Context;
use native_tls::TlsConnector;
use parquet::basic::LogicalType;
use parquet::schema::types::Type::{GroupType, PrimitiveType};
use postgres_native_tls::MakeTlsConnector;
use std::sync::Arc;

pub async fn create_connection(conn_url: &str) -> Result<tokio_postgres::Client, anyhow::Error> {
    let connector = TlsConnector::new().unwrap();
    let connector = MakeTlsConnector::new(connector);
    let (client, connection) = tokio_postgres::connect(conn_url, connector).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
}

pub fn build_fqtn<'a>(table_name: &'a String) -> Result<(&'a str, &'a str), anyhow::Error> {
    let fqtn_tup = table_name.split_once('.');
    match fqtn_tup {
        Some((schema, table)) => Ok((schema, table)),
        None => Err(anyhow::Error::msg("Invalid table name")),
    }
}
/*
pub fn build_ddl(
    fields: &Vec<Arc<parquet::schema::types::Type>>,
    user_defs: &[ColumnDefintion],
) -> String {
    let mapped = fields
        .iter()
        .filter_map(|pq| map_parquet_to_ddl(pq, user_defs))
        .collect::<Vec<(String, postgres_types::Type)>>();
}*/

pub fn map_parquet_to_ddl(
    pq: &Arc<parquet::schema::types::Type>,
    user_defs: &[ColumnDefintion],
) -> Option<(String, postgres_types::Type)> {
    match pq.as_ref() {
        GroupType { .. } => None,
        PrimitiveType { basic_info, .. } => {
            let name = pq.name();
            let pg_type = match basic_info {
                bi if bi.logical_type().is_some() => {
                    // we have a logical type, use that
                    match bi.logical_type().unwrap() {
                        LogicalType::String => user_defs
                            .iter()
                            .find(|cd| cd.name == name)
                            .and_then(|cd| {
                                if cd.ty == "VARCHAR" {
                                    Some((
                                        format!("VARCHAR ({})", cd.len),
                                        postgres_types::Type::VARCHAR,
                                    ))
                                } else {
                                    None
                                }
                            })
                            .unwrap_or((String::from("TEXT"), postgres_types::Type::TEXT)),
                        LogicalType::Timestamp {
                            is_adjusted_to_u_t_c,
                            ..
                        } => {
                            if is_adjusted_to_u_t_c {
                                (
                                    String::from("TIMESTAMPTZ"),
                                    postgres_types::Type::TIMESTAMPTZ,
                                )
                            } else {
                                (String::from("TIMESTAMP"), postgres_types::Type::TIMESTAMP)
                            }
                        }
                        LogicalType::Uuid => (String::from("UUID"), postgres_types::Type::UUID),
                        LogicalType::Json => (String::from("JSONB"), postgres_types::Type::JSONB),
                        LogicalType::Decimal { scale, precision } => (
                            format!("NUMERIC({},{})", precision, scale),
                            postgres_types::Type::NUMERIC,
                        ),
                        _ => (String::from("TEXT"), postgres_types::Type::TEXT),
                    }
                }
                _ => match pq.get_physical_type() {
                    parquet::basic::Type::FLOAT => {
                        (String::from("FLOAT4"), postgres_types::Type::FLOAT4)
                    }
                    parquet::basic::Type::DOUBLE => {
                        (String::from("FLOAT8"), postgres_types::Type::FLOAT8)
                    }
                    parquet::basic::Type::INT32 => {
                        (String::from("INT4"), postgres_types::Type::INT4)
                    }
                    parquet::basic::Type::INT64 => {
                        (String::from("INT8"), postgres_types::Type::INT8)
                    }
                    parquet::basic::Type::BOOLEAN => {
                        (String::from("BOOL"), postgres_types::Type::BOOL)
                    }
                    _ => (String::from("TEXT"), postgres_types::Type::TEXT),
                },
            };
            Some((format!("{} {}", name, pg_type.0), pg_type.1))
        }
    }
}
