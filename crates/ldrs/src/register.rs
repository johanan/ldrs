use std::ffi::OsString;

use anyhow::Context;
use futures::future::join_all;
use ldrs_core::phase::{DestinationOutcome, PhaseOutput};
use ldrs_storage::store_location;
use serde::Deserialize;
use tracing::{debug, info};
use url::Url;

use crate::delta::{DeltaDestination, Refresh, RegisterSpec};
use crate::ldrs_config::config::LdrsDestination;
use crate::ldrs_config::get_dest_url;
use crate::ldrs_env::{ambient_env, LdrsExecutionContext};
use crate::ldrs_snowflake::{
    resolve_conn_creds, resolve_inherited_sf_env, SnowflakeConnection, StatementResult,
};
use crate::results::{ResultLine, Results};

pub(crate) struct ResolvedSfRegister {
    pub table: String,
    /// Everything before the last dot. `None` leaves the scope to the connection's own.
    pub schema_path: Option<String>,
    pub name: String,
    pub integration: String,
    pub external_volume: String,
    pub refresh: Refresh,
    pub conn: SnowflakeConnection,
}

fn delta_register(dest: &DeltaDestination) -> Option<&RegisterSpec> {
    match dest {
        DeltaDestination::Overwrite(c) => c.register.as_ref(),
        DeltaDestination::Merge(m) => m.common.register.as_ref(),
    }
}

/// One entry per destination, so the result lines up with the outcomes the load returns.
pub(crate) fn resolve_registers(
    dests: &[LdrsDestination],
    context: &LdrsExecutionContext<'_>,
    ldrs_env: &[(String, String)],
) -> Result<Vec<Option<ResolvedSfRegister>>, anyhow::Error> {
    dests
        .iter()
        .map(|dest| {
            let LdrsDestination::Delta(delta) = dest else {
                return Ok(None);
            };
            match delta_register(delta) {
                None => Ok(None),
                Some(RegisterSpec::Snowflake(sf)) => {
                    let table = context.render_template(&sf.table)?;
                    let (schema_path, name) = match table.rsplit_once('.') {
                        Some((path, name)) => (Some(path.to_string()), name.to_string()),
                        None => (None, table.clone()),
                    };
                    // The rendered table is the connection ident, so it walks the usual tier chain.
                    let (key, url) = get_dest_url(ldrs_env, &table, "SF")?;
                    let (pem_key, pem_file) = resolve_conn_creds(ldrs_env, key);
                    Ok(Some(ResolvedSfRegister {
                        schema_path,
                        name,
                        integration: context.render_template(&sf.integration)?,
                        external_volume: context.render_template(&sf.external_volume)?,
                        refresh: sf.refresh,
                        conn: SnowflakeConnection::create_connection(
                            url,
                            pem_key,
                            pem_file,
                            resolve_inherited_sf_env(ldrs_env),
                        )?,
                        table,
                    }))
                }
            }
        })
        .collect()
}

/// A register runs only when its own destination committed, so `registers` is indexed to match.
pub(crate) async fn run_register(
    registers: &[Option<ResolvedSfRegister>],
    phase: &PhaseOutput,
    results: &Results,
) -> Vec<String> {
    let ambient = ambient_env();
    let runs = registers.iter().zip(&phase.destinations).map(|pair| {
        let ambient = ambient.clone();
        async move {
            match pair {
                (
                    Some(register),
                    DestinationOutcome::Delta {
                        target,
                        url,
                        result: Ok(_),
                        ..
                    },
                ) => {
                    let outcome = register_sf(register, url, ambient).await;
                    if let Ok(action) = &outcome {
                        info!(target = %target, catalog_table = %register.table, action, "registered");
                    }
                    results
                        .write(ResultLine::Register {
                            name: &phase.name,
                            target,
                            catalog_table: &register.table,
                            result: outcome.as_ref().copied().map_err(String::as_str),
                        })
                        .map_err(|e| format!("{e:#}"))?;
                    outcome.map(|_| ())
                }
                _ => Ok(()),
            }
        }
    });
    join_all(runs)
        .await
        .into_iter()
        .filter_map(Result::err)
        .collect()
}

#[derive(Deserialize)]
struct PropertyRow {
    /// Absent from DESCRIBE CATALOG INTEGRATION, which has no nested properties.
    #[serde(default)]
    parent_property: String,
    property: String,
    property_value: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct StorageLocation {
    storage_base_url: String,
}

#[derive(Debug, Deserialize)]
struct IcebergTableRow {
    external_volume_name: String,
    catalog_name: String,
    base_location: String,
    iceberg_table_type: String,
    /// Empty when the table is not auto-refreshing; a JSON status object when it is.
    auto_refresh_status: String,
}

#[derive(Debug)]
struct Probe {
    volume_base_urls: Vec<String>,
    existing: Option<IcebergTableRow>,
}

fn read_probe(results: &[StatementResult]) -> Result<Probe, anyhow::Error> {
    match results {
        [volume, integration, show] => {
            // The other two result sets are bounded; only this one grows with configuration.
            match volume.truncated {
                true => Err(anyhow::anyhow!(
                    "the external volume reported more locations than ldrs-sf captured"
                )),
                false => Ok(()),
            }?;

            let volume_base_urls = volume
                .rows_as::<PropertyRow>()?
                .iter()
                // the sibling ACTIVE row shares the parent but holds a name, not JSON
                .filter(|row| {
                    row.parent_property == "STORAGE_LOCATIONS"
                        && row.property.starts_with("STORAGE_LOCATION_")
                })
                .map(|row| {
                    serde_json::from_str::<StorageLocation>(&row.property_value)
                        .map(|location| location.storage_base_url)
                        .with_context(|| {
                            format!("could not read storage location '{}'", row.property)
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;

            check_integration(integration)?;

            Ok(Probe {
                volume_base_urls,
                existing: show.rows_as::<IcebergTableRow>()?.into_iter().next(),
            })
        }
        other => Err(anyhow::anyhow!(
            "expected three probe result sets, got {}",
            other.len()
        )),
    }
}

fn check_integration(result: &StatementResult) -> Result<(), anyhow::Error> {
    let properties = result.rows_as::<PropertyRow>()?;
    let value = |name: &str| {
        properties
            .iter()
            .find(|row| row.property == name)
            .map(|row| row.property_value.as_str())
    };
    match (
        value("ENABLED"),
        value("CATALOG_SOURCE"),
        value("TABLE_FORMAT"),
    ) {
        (Some("true"), Some("OBJECT_STORE"), Some("DELTA")) => Ok(()),
        (Some("false"), _, _) => Err(anyhow::anyhow!("the catalog integration is disabled")),
        (_, source, format) => Err(anyhow::anyhow!(
            "the catalog integration is '{}' / '{}', not OBJECT_STORE / DELTA",
            source.unwrap_or("unset"),
            format.unwrap_or("unset")
        )),
    }
}

fn create_statement(register: &ResolvedSfRegister, base_location: &str) -> String {
    let auto = match register.refresh {
        Refresh::Auto => "AUTO_REFRESH = TRUE",
        Refresh::Load => "",
    };
    format!(
        "CREATE ICEBERG TABLE IF NOT EXISTS {} CATALOG = {} EXTERNAL_VOLUME = {} BASE_LOCATION = '{base_location}' {auto}",
        register.table, register.integration, register.external_volume
    )
}

fn refresh_statement(register: &ResolvedSfRegister) -> String {
    format!("ALTER ICEBERG TABLE {} REFRESH", register.table)
}

/// The statements that bring the catalog in line with the config, or the mismatch that stops us.
fn write_statements(
    register: &ResolvedSfRegister,
    base_location: &str,
    existing: Option<&IcebergTableRow>,
) -> Result<Vec<String>, anyhow::Error> {
    let table = &register.table;
    match existing {
        None => Ok(match register.refresh {
            Refresh::Load => vec![
                create_statement(register, base_location),
                refresh_statement(register),
            ],
            Refresh::Auto => vec![create_statement(register, base_location)],
        }),

        Some(row)
            if !row
                .external_volume_name
                .eq_ignore_ascii_case(&register.external_volume) =>
        {
            Err(anyhow::anyhow!(
                "'{table}' is registered against external volume '{}', not '{}'",
                row.external_volume_name,
                register.external_volume
            ))
        }
        Some(row) if !row.catalog_name.eq_ignore_ascii_case(&register.integration) => {
            Err(anyhow::anyhow!(
                "'{table}' is registered against catalog '{}', not '{}'",
                row.catalog_name,
                register.integration
            ))
        }
        Some(row) if row.base_location != base_location => Err(anyhow::anyhow!(
            "'{table}' is registered at '{}', not '{base_location}'",
            row.base_location
        )),
        Some(row) if row.iceberg_table_type != "UNMANAGED" => Err(anyhow::anyhow!(
            "'{table}' is a {} iceberg table, not an externally managed one",
            row.iceberg_table_type
        )),

        // present and ours, so only the refresh mode can still need changing
        Some(row) => Ok(match (register.refresh, row.auto_refresh_status.as_str()) {
            (Refresh::Load, "") => vec![refresh_statement(register)],
            (Refresh::Load, _) => vec![
                format!("ALTER ICEBERG TABLE {table} SET AUTO_REFRESH = FALSE"),
                refresh_statement(register),
            ],
            (Refresh::Auto, "") => {
                vec![format!(
                    "ALTER ICEBERG TABLE {table} SET AUTO_REFRESH = TRUE"
                )]
            }
            (Refresh::Auto, _) => vec![],
        }),
    }
}

/// The path Snowflake appends to the external volume's own base url to reach the table.
fn derive_base_location(probe: &Probe, full_url: &str) -> Result<String, anyhow::Error> {
    let table = store_location(&Url::parse(full_url)?)?;

    let matched: Vec<String> = probe
        .volume_base_urls
        .iter()
        .map(|base| store_location(&Url::parse(base)?))
        .collect::<Result<Vec<_>, anyhow::Error>>()?
        .iter()
        .filter(|volume| volume.base == table.base)
        .filter_map(|volume| table.path.strip_prefix(&volume.path).map(String::from))
        .collect();

    match matched.as_slice() {
        [base_location] => Ok(base_location.clone()),
        [] => Err(anyhow::anyhow!(
            "'{full_url}' is not inside the external volume ({})",
            probe.volume_base_urls.join(", ")
        )),
        many => Err(anyhow::anyhow!(
            "'{full_url}' is inside {} of the external volume's locations",
            many.len()
        )),
    }
}

fn probe_statements(register: &ResolvedSfRegister) -> [String; 3] {
    let show = match &register.schema_path {
        Some(path) => format!(
            "SHOW ICEBERG TABLES LIKE '{}' IN SCHEMA {path}",
            register.name
        ),
        None => format!("SHOW ICEBERG TABLES LIKE '{}'", register.name),
    };
    [
        format!("DESCRIBE EXTERNAL VOLUME {}", register.external_volume),
        format!("DESCRIBE CATALOG INTEGRATION {}", register.integration),
        show,
    ]
}

pub(crate) async fn register_sf(
    register: &ResolvedSfRegister,
    full_url: &str,
    ambient: Vec<(String, OsString)>,
) -> Result<&'static str, String> {
    let conn = register.conn.clone();
    let probe_batch = probe_statements(register);
    let probe_ambient = ambient.clone();
    let results = tokio::task::spawn_blocking(move || conn.exec(&probe_batch, probe_ambient))
        .await
        .map_err(|e| format!("register task panicked: {e}"))?
        .map_err(|e| format!("{e:#}"))?;
    let probe = read_probe(&results).map_err(|e| format!("{e:#}"))?;

    let base_location = derive_base_location(&probe, full_url).map_err(|e| format!("{e:#}"))?;
    let statements = write_statements(register, &base_location, probe.existing.as_ref())
        .map_err(|e| format!("{e:#}"))?;

    let action = match (&probe.existing, statements.as_slice()) {
        (None, _) => "created",
        (Some(_), []) => "unchanged",
        (Some(_), _) => "refreshed",
    };

    match statements.as_slice() {
        // already registered and already in the configured refresh mode
        [] => Ok(action),
        _ => {
            debug!(?statements, "register statements");
            let conn = register.conn.clone();
            tokio::task::spawn_blocking(move || conn.exec(&statements, ambient))
                .await
                .map_err(|e| format!("register task panicked: {e}"))?
                .map_err(|e| format!("{e:#}"))?;
            Ok(action)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    fn conn() -> SnowflakeConnection {
        SnowflakeConnection {
            conn_url: Url::parse("snowflake://acct").unwrap(),
            raw_conn_url: "snowflake://acct".to_string(),
            binary_path: PathBuf::from("ldrs-sf"),
            pem_key: None,
            pem_file: None,
            inherited_sf_env: Vec::new(),
        }
    }

    fn register(refresh: Refresh) -> ResolvedSfRegister {
        ResolvedSfRegister {
            table: "DB.SCH.TBL".to_string(),
            schema_path: Some("DB.SCH".to_string()),
            name: "TBL".to_string(),
            integration: "cat_int".to_string(),
            external_volume: "vol".to_string(),
            refresh,
            conn: conn(),
        }
    }

    fn existing(auto_refresh_status: &str) -> IcebergTableRow {
        IcebergTableRow {
            external_volume_name: "VOL".to_string(),
            catalog_name: "CAT_INT".to_string(),
            base_location: "one/two/tbl/".to_string(),
            iceberg_table_type: "UNMANAGED".to_string(),
            auto_refresh_status: auto_refresh_status.to_string(),
        }
    }

    fn result(columns: &[&str], rows: Vec<Vec<serde_json::Value>>) -> StatementResult {
        StatementResult {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            rows,
            truncated: false,
        }
    }

    /// Shaped as `DESCRIBE EXTERNAL VOLUME` returns it, sibling ACTIVE row included.
    fn volume_result(base_urls: &[&str]) -> StatementResult {
        let mut rows = vec![json!(["", "ALLOW_WRITES", "Boolean", "false", "true"])
            .as_array()
            .unwrap()
            .clone()];
        for (i, url) in base_urls.iter().enumerate() {
            let location = json!({ "NAME": format!("loc{i}"), "STORAGE_PROVIDER": "AZURE",
                                   "STORAGE_BASE_URL": url })
            .to_string();
            rows.push(vec![
                json!("STORAGE_LOCATIONS"),
                json!(format!("STORAGE_LOCATION_{}", i + 1)),
                json!("String"),
                json!(location),
                json!(""),
            ]);
        }
        rows.push(vec![
            json!("STORAGE_LOCATIONS"),
            json!("ACTIVE"),
            json!("String"),
            json!("loc0"),
            json!(""),
        ]);
        result(
            &[
                "parent_property",
                "property",
                "property_type",
                "property_value",
                "property_default",
            ],
            rows,
        )
    }

    fn integration_result(enabled: &str, source: &str, format: &str) -> StatementResult {
        result(
            &[
                "property",
                "property_type",
                "property_value",
                "property_default",
            ],
            vec![
                vec![
                    json!("ENABLED"),
                    json!("Boolean"),
                    json!(enabled),
                    json!("true"),
                ],
                vec![
                    json!("CATALOG_SOURCE"),
                    json!("String"),
                    json!(source),
                    json!(""),
                ],
                vec![
                    json!("TABLE_FORMAT"),
                    json!("String"),
                    json!(format),
                    json!(""),
                ],
            ],
        )
    }

    fn show_result(rows: Vec<Vec<serde_json::Value>>) -> StatementResult {
        result(
            &[
                "name",
                "external_volume_name",
                "catalog_name",
                "iceberg_table_type",
                "base_location",
                "auto_refresh_status",
            ],
            rows,
        )
    }

    fn probe(volume_base_urls: &[&str]) -> Probe {
        Probe {
            volume_base_urls: volume_base_urls.iter().map(|u| u.to_string()).collect(),
            existing: None,
        }
    }

    #[test]
    fn derive_takes_the_whole_path_from_a_volume_at_the_container_root() {
        let got = derive_base_location(
            &probe(&["azure://acct.blob.core.windows.net/cont/"]),
            "https://acct.blob.core.windows.net/cont/one/two/tbl",
        )
        .unwrap();
        assert_eq!(got, "one/two/tbl/");
    }

    #[test]
    fn derive_subtracts_a_scoped_volume_path() {
        let got = derive_base_location(
            &probe(&["azure://acct.blob.core.windows.net/cont/one/"]),
            "https://acct.blob.core.windows.net/cont/one/two/tbl",
        )
        .unwrap();
        assert_eq!(got, "two/tbl/");
    }

    #[test]
    fn derive_picks_the_one_location_that_contains_the_table() {
        let got = derive_base_location(
            &probe(&[
                "azure://acct.blob.core.windows.net/other/",
                "azure://acct.blob.core.windows.net/cont/one/",
                "azure://acct.blob.core.windows.net/cont/nine/",
            ]),
            "https://acct.blob.core.windows.net/cont/one/two/tbl",
        )
        .unwrap();
        assert_eq!(got, "two/tbl/");
    }

    #[test]
    fn derive_rejects_a_table_in_another_container() {
        let err = derive_base_location(
            &probe(&["azure://acct.blob.core.windows.net/cont/"]),
            "https://acct.blob.core.windows.net/other/one/tbl",
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("not inside the external volume"));
    }

    /// Same container name under a different storage account is a different container.
    #[test]
    fn derive_rejects_a_table_in_another_account() {
        let err = derive_base_location(
            &probe(&["azure://acct.blob.core.windows.net/cont/"]),
            "https://other.blob.core.windows.net/cont/one/tbl",
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("not inside the external volume"));
    }

    #[test]
    fn derive_rejects_nested_locations_that_both_contain_the_table() {
        let err = derive_base_location(
            &probe(&[
                "azure://acct.blob.core.windows.net/cont/",
                "azure://acct.blob.core.windows.net/cont/one/",
            ]),
            "https://acct.blob.core.windows.net/cont/one/two/tbl",
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("is inside 2"));
    }

    #[test]
    fn derive_rejects_a_destination_that_is_not_azure() {
        let err = derive_base_location(
            &probe(&["azure://acct.blob.core.windows.net/cont/"]),
            "s3://bucket/one/tbl",
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("not an azure url"));
    }

    #[test]
    fn read_probe_skips_the_active_row_and_reads_the_locations() {
        let probe = read_probe(&[
            volume_result(&["azure://acct.blob.core.windows.net/cont/"]),
            integration_result("true", "OBJECT_STORE", "DELTA"),
            show_result(vec![]),
        ])
        .unwrap();
        assert_eq!(
            probe.volume_base_urls,
            vec!["azure://acct.blob.core.windows.net/cont/"]
        );
        assert!(
            probe.existing.is_none(),
            "no rows means the table is absent"
        );
    }

    #[test]
    fn read_probe_reads_every_location() {
        let probe = read_probe(&[
            volume_result(&[
                "azure://acct.blob.core.windows.net/one/",
                "azure://acct.blob.core.windows.net/two/",
            ]),
            integration_result("true", "OBJECT_STORE", "DELTA"),
            show_result(vec![]),
        ])
        .unwrap();
        assert_eq!(probe.volume_base_urls.len(), 2);
    }

    #[test]
    fn read_probe_reads_the_existing_table() {
        let probe = read_probe(&[
            volume_result(&["azure://acct.blob.core.windows.net/cont/"]),
            integration_result("true", "OBJECT_STORE", "DELTA"),
            show_result(vec![show_row("one/two/tbl/", "")]),
        ])
        .unwrap();
        let row = probe
            .existing
            .expect("one row means the table is registered");
        assert_eq!(row.base_location, "one/two/tbl/");
        assert_eq!(row.iceberg_table_type, "UNMANAGED");
        assert_eq!(row.auto_refresh_status, "");
    }

    #[test]
    fn read_probe_rejects_a_truncated_volume() {
        let mut volume = volume_result(&["azure://acct.blob.core.windows.net/cont/"]);
        volume.truncated = true;
        let err = read_probe(&[
            volume,
            integration_result("true", "OBJECT_STORE", "DELTA"),
            show_result(vec![]),
        ])
        .unwrap_err();
        assert!(format!("{err:#}").contains("captured"));
    }

    #[test]
    fn read_probe_rejects_a_disabled_integration() {
        let err = read_probe(&[
            volume_result(&["azure://acct.blob.core.windows.net/cont/"]),
            integration_result("false", "OBJECT_STORE", "DELTA"),
            show_result(vec![]),
        ])
        .unwrap_err();
        assert!(format!("{err:#}").contains("disabled"));
    }

    #[test]
    fn read_probe_rejects_the_wrong_table_format() {
        let err = read_probe(&[
            volume_result(&["azure://acct.blob.core.windows.net/cont/"]),
            integration_result("true", "OBJECT_STORE", "ICEBERG"),
            show_result(vec![]),
        ])
        .unwrap_err();
        let message = format!("{err:#}");
        assert!(message.contains("ICEBERG"), "{message}");
    }

    #[test]
    fn read_probe_rejects_the_wrong_number_of_result_sets() {
        let err = read_probe(&[integration_result("true", "OBJECT_STORE", "DELTA")]).unwrap_err();
        assert!(format!("{err:#}").contains("got 1"));
    }

    #[test]
    fn absent_under_load_creates_then_refreshes() {
        let got = write_statements(&register(Refresh::Load), "one/two/tbl/", None).unwrap();
        assert_eq!(got.len(), 2);
        assert!(got[0].starts_with("CREATE ICEBERG TABLE IF NOT EXISTS DB.SCH.TBL"));
        assert!(got[0].contains("BASE_LOCATION = 'one/two/tbl/'"));
        assert!(!got[0].contains("AUTO_REFRESH"));
        assert_eq!(got[1], "ALTER ICEBERG TABLE DB.SCH.TBL REFRESH");
    }

    #[test]
    fn absent_under_auto_creates_only() {
        let got = write_statements(&register(Refresh::Auto), "one/two/tbl/", None).unwrap();
        assert_eq!(got.len(), 1);
        assert!(got[0].contains("AUTO_REFRESH = TRUE"));
    }

    #[test]
    fn present_under_load_refreshes() {
        let row = existing("");
        let got = write_statements(&register(Refresh::Load), "one/two/tbl/", Some(&row)).unwrap();
        assert_eq!(got, vec!["ALTER ICEBERG TABLE DB.SCH.TBL REFRESH"]);
    }

    #[test]
    fn present_under_load_disables_auto_before_refreshing() {
        let row = existing(r#"{"executionState":"RUNNING"}"#);
        let got = write_statements(&register(Refresh::Load), "one/two/tbl/", Some(&row)).unwrap();
        assert_eq!(
            got,
            vec![
                "ALTER ICEBERG TABLE DB.SCH.TBL SET AUTO_REFRESH = FALSE",
                "ALTER ICEBERG TABLE DB.SCH.TBL REFRESH",
            ]
        );
    }

    #[test]
    fn present_under_auto_enables_it() {
        let row = existing("");
        let got = write_statements(&register(Refresh::Auto), "one/two/tbl/", Some(&row)).unwrap();
        assert_eq!(
            got,
            vec!["ALTER ICEBERG TABLE DB.SCH.TBL SET AUTO_REFRESH = TRUE"]
        );
    }

    #[test]
    fn present_under_auto_already_polling_has_nothing_to_do() {
        let row = existing(r#"{"executionState":"RUNNING"}"#);
        let got = write_statements(&register(Refresh::Auto), "one/two/tbl/", Some(&row)).unwrap();
        assert!(
            got.is_empty(),
            "an empty batch is what skips the second spawn"
        );
    }

    /// Snowflake upcases unquoted identifiers.
    #[test]
    fn identifiers_compare_case_insensitively() {
        let row = existing("");
        assert!(write_statements(&register(Refresh::Load), "one/two/tbl/", Some(&row)).is_ok());
    }

    #[test]
    fn a_different_volume_is_a_mismatch() {
        let mut row = existing("");
        row.external_volume_name = "OTHER_VOL".to_string();
        let err =
            write_statements(&register(Refresh::Load), "one/two/tbl/", Some(&row)).unwrap_err();
        let message = format!("{err:#}");
        assert!(message.contains("OTHER_VOL"), "{message}");
        assert!(message.contains("vol"), "{message}");
    }

    #[test]
    fn a_different_catalog_is_a_mismatch() {
        let mut row = existing("");
        row.catalog_name = "OTHER_CAT".to_string();
        let err =
            write_statements(&register(Refresh::Load), "one/two/tbl/", Some(&row)).unwrap_err();
        assert!(format!("{err:#}").contains("OTHER_CAT"));
    }

    #[test]
    fn a_different_base_location_is_a_mismatch() {
        let row = existing("");
        let err =
            write_statements(&register(Refresh::Load), "one/two/other/", Some(&row)).unwrap_err();
        let message = format!("{err:#}");
        assert!(message.contains("one/two/tbl/"), "{message}");
        assert!(message.contains("one/two/other/"), "{message}");
    }

    #[test]
    fn a_snowflake_managed_table_is_a_mismatch() {
        let mut row = existing("");
        row.iceberg_table_type = "MANAGED".to_string();
        let err =
            write_statements(&register(Refresh::Load), "one/two/tbl/", Some(&row)).unwrap_err();
        assert!(format!("{err:#}").contains("MANAGED"));
    }

    fn show_row(base_location: &str, auto_refresh_status: &str) -> Vec<serde_json::Value> {
        vec![
            json!("TBL"),
            json!("VOL"),
            json!("CAT_INT"),
            json!("UNMANAGED"),
            json!(base_location),
            json!(auto_refresh_status),
        ]
    }
}
