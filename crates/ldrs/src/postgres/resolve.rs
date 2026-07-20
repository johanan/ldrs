//! Resolve Postgres command templates into executable `ldrs_postgres::Command`s: render the
//! Handlebars templates and bind prepared-statement params from the environment.

use ldrs_postgres::Command;

use crate::ldrs_config::get_env_value;
use crate::ldrs_env::{shouty, LdrsExecutionContext};
use crate::postgres::postgres_destination::PgDestCommand;

/// Render one command's templates and, for a prepared statement, bind its params. Called per
/// command across a load's pre-load and post-load sequences.
pub fn resolve_command(
    cmd: &PgDestCommand,
    ctx: &LdrsExecutionContext<'_>,
    ldrs_env: &[(String, String)],
    target: &str,
) -> Result<Command, anyhow::Error> {
    Ok(match cmd {
        PgDestCommand::Sql(sql) => Command::Sql(ctx.render_template(sql)?),
        PgDestCommand::CreateTable(t) => Command::CreateTable(ctx.render_template(t)?),
        PgDestCommand::CreateTempTable(t) => Command::CreateTempTable(ctx.render_template(t)?),
        PgDestCommand::Merge(m) => Command::Merge {
            target: ctx.render_template(&m.target)?,
            source: ctx.render_template(&m.source)?,
            keys: m.keys.clone(),
        },
        PgDestCommand::Prepared(p) => Command::Prepared {
            stmt: ctx.render_template(&p.stmt)?,
            params: resolve_params(&p.keys, ldrs_env, target)?,
        },
        PgDestCommand::Load(_) => {
            unreachable!("split_pg_plan extracts the Load; it never reaches command resolution")
        }
    })
}

/// Resolve each delete-key param to its `(name, value)`: value from `LDRS_PARAM_<TABLE>_<COL>`
/// (falling back to `LDRS_PARAM_<COL>`, most-specific first like the src/dest lookups). Ordered,
/// so binds line up with `$1..$n`; a missing value is a hard error.
fn resolve_params(
    keys: &[String],
    ldrs_env: &[(String, String)],
    target: &str,
) -> Result<Vec<(String, String)>, anyhow::Error> {
    keys.iter()
        .map(|key| {
            let scoped = format!("LDRS_PARAM_{}_{}", shouty(target), shouty(key));
            let general = format!("LDRS_PARAM_{}", shouty(key));
            let value = get_env_value(ldrs_env, &[scoped.as_str(), general.as_str()])
                .map(|(_, v)| v.clone())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "missing param for delete key '{key}': set {scoped} or {general}"
                    )
                })?;
            Ok((key.clone(), value))
        })
        .collect()
}
