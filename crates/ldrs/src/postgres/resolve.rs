//! Resolve Postgres command templates into executable `ldrs_postgres::Command`s: render the
//! Handlebars templates and bind prepared-statement params from the environment. Also reads the
//! role out of a connection URL's libpq `options`.

use ldrs_postgres::Command;
use nom::{
    branch::alt,
    bytes::complete::{escaped_transform, is_not, tag},
    character::complete::{anychar, multispace0, multispace1},
    combinator::map,
    multi::separated_list0,
    sequence::{delimited, preceded, separated_pair},
    IResult, Parser,
};

use crate::ldrs_config::get_env_value;
use crate::ldrs_env::{shouty, LdrsExecutionContext};
use crate::postgres::postgres_destination::PgDestCommand;

/// One argument: an unescaped space ends it, `\x` yields `x`, and `\\` a literal backslash.
fn argument(input: &str) -> IResult<&str, String> {
    escaped_transform(is_not("\\ "), '\\', anychar).parse(input)
}

/// `name=value`, where the name stops at the first unescaped `=`.
fn assignment(input: &str) -> IResult<&str, (String, String)> {
    separated_pair(
        escaped_transform(is_not("\\ ="), '\\', anychar),
        tag("="),
        argument,
    )
    .parse(input)
}

/// A setting in any form the server accepts: `-c name=value`, `-cname=value`, `--name=value`.
fn setting(input: &str) -> IResult<&str, (String, String)> {
    preceded(
        alt((preceded(tag("-c"), multispace1), tag("-c"), tag("--"))),
        assignment,
    )
    .parse(input)
}

/// A setting, or any other argument, which is skipped: `options` can carry arguments ldrs does not
/// model (`-S 1000`), and a setting following one still has to be found.
fn item(input: &str) -> IResult<&str, Option<(String, String)>> {
    alt((map(setting, Some), map(argument, |_| None))).parse(input)
}

/// Every `name=value` setting in a libpq `options` string. Spaces separate arguments unless escaped
/// with a backslash. libpq forwards the string to the server as argv and never parses it, so there
/// is no upstream form to read.
fn settings(options: &str) -> Vec<(String, String)> {
    delimited(multispace0, separated_list0(multispace1, item), multispace0)
        .parse(options)
        .map(|(_, items)| items.into_iter().flatten().collect())
        .unwrap_or_default()
}

/// The role a connection URL's `options` sets, if any. The last occurrence wins, as it does
/// server-side. Applied per transaction with `SET LOCAL ROLE`, so it survives pool recycling.
fn role_from_options(options: &str) -> Option<String> {
    settings(options)
        .into_iter()
        .filter_map(|(name, value)| (name == "role").then_some(value))
        .last()
}

/// The role a Postgres connection URL carries in its libpq `options`, the supported spelling of the
/// deprecated `role` query parameter.
pub fn role_from_url(url: &str) -> Option<String> {
    let config = url.parse::<tokio_postgres::Config>().ok()?;
    config.get_options().and_then(role_from_options)
}

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

#[cfg(test)]
mod role_tests {
    use super::*;

    #[test]
    fn every_form_the_server_accepts() {
        assert_eq!(
            role_from_options("-c role=reader").as_deref(),
            Some("reader")
        );
        assert_eq!(
            role_from_options("-crole=reader").as_deref(),
            Some("reader")
        );
        assert_eq!(
            role_from_options("--role=reader").as_deref(),
            Some("reader")
        );
    }

    #[test]
    fn the_last_occurrence_wins_as_it_does_server_side() {
        assert_eq!(
            role_from_options("-c role=first --role=second -c role=third").as_deref(),
            Some("third")
        );
    }

    #[test]
    fn other_settings_and_unmodelled_arguments_are_skipped() {
        let options = "-S 1000 -c search_path=app -c role=reader -c statement_timeout=5000";
        assert_eq!(role_from_options(options).as_deref(), Some("reader"));
        assert_eq!(role_from_options("-c search_path=app"), None);
        assert_eq!(role_from_options(""), None);
        assert_eq!(role_from_options("   "), None);
    }

    #[test]
    fn a_backslash_escaped_space_stays_in_the_value() {
        assert_eq!(
            role_from_options(r"-c role=two\ words").as_deref(),
            Some("two words")
        );
        assert_eq!(
            role_from_options(r"-c role=back\\slash").as_deref(),
            Some(r"back\slash")
        );
    }

    #[test]
    fn read_from_a_whole_connection_url() {
        assert_eq!(
            role_from_url("postgres://u@h/db?options=-c%20role%3Dreader").as_deref(),
            Some("reader")
        );
        assert_eq!(role_from_url("postgres://u@h/db"), None);
        assert_eq!(role_from_url("not a url"), None);
    }
}
