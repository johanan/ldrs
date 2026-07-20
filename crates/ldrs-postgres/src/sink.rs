use std::pin::Pin;

use anyhow::Context;
use arrow_array::RecordBatch;
use deadpool_postgres::Object;
use ldrs_arrow::{ColumnSpec, TypedColumnAccessor};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::Client;

use crate::extracted_values::{ColumnConverter, ExtractedValue};
use crate::map_colspec_to_pg_type;
use crate::postgres_execution::{
    execute_create_table, execute_create_temp_table, execute_merge, execute_prepared_stmt,
    execute_sql,
};

/// Where record batches land in Postgres: a binary `COPY IN` over an owned, pooled
/// connection.
pub struct PgCopySink {
    conn: Object,
    writer: Pin<Box<BinaryCopyInWriter>>,
    final_cols: Vec<ColumnSpec>,
}

impl PgCopySink {
    /// Take ownership of a connection and open the binary COPY on it.
    pub async fn open(
        conn: Object,
        load_table: &str,
        final_cols: Vec<ColumnSpec>,
    ) -> Result<Self, anyhow::Error> {
        let pg_types = final_cols
            .iter()
            .map(map_colspec_to_pg_type)
            .collect::<Vec<_>>();
        let binary_ddl = format!("COPY {} FROM STDIN WITH (FORMAT BINARY)", load_table);
        let sink = conn
            .copy_in(&binary_ddl)
            .await
            .with_context(|| "Could not create binary copy")?;
        let writer = Box::pin(BinaryCopyInWriter::new(sink, &pg_types));

        Ok(PgCopySink {
            conn,
            writer,
            final_cols,
        })
    }

    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), anyhow::Error> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let accessors: Vec<TypedColumnAccessor> = batch
            .columns()
            .iter()
            .map(TypedColumnAccessor::new)
            .collect();
        let converters = accessors
            .iter()
            .zip(self.final_cols.iter())
            .map(|(accessor, col)| ColumnConverter::new(accessor, col))
            .collect::<Result<Vec<_>, _>>()?;

        let mut row_buffer = Vec::<ExtractedValue>::with_capacity(batch.num_columns());
        for row_idx in 0..batch.num_rows() {
            row_buffer.clear();
            for converter in converters.iter() {
                row_buffer.push(converter.extract_value(row_idx));
            }
            self.writer
                .as_mut()
                .write_raw(&row_buffer)
                .await
                .with_context(|| "Failed to write row to PostgreSQL")?;
        }
        Ok(())
    }

    pub async fn finish(self) -> Result<Object, anyhow::Error> {
        let PgCopySink {
            conn, mut writer, ..
        } = self;
        writer
            .as_mut()
            .finish()
            .await
            .with_context(|| "Could not finish copy")?;
        Ok(conn)
    }

    /// Abandon the COPY and hand the connection back.
    pub fn cancel(self) -> Object {
        // `writer` drops here, sending CopyFail; `conn` is moved out and returned.
        self.conn
    }
}

/// A resolved pre-/post-load command: literal SQL and bound params. No templates, no lookup keys
#[derive(Debug, Clone)]
pub enum Command {
    Sql(String),
    Prepared {
        stmt: String,
        /// `(column name, value)` per bind, in `$1..$n` order. The value is the string from the
        /// environment; the type is resolved at execution from the target columns by name.
        params: Vec<(String, String)>,
    },
    CreateTable(String),
    CreateTempTable(String),
    Merge {
        target: String,
        source: String,
        keys: Vec<String>,
    },
}

/// A fully-resolved Postgres load: the pre-load sequence, the COPY target, the post-load
/// sequence, the final columns, and the destination identity.
#[derive(Debug, Clone)]
pub struct PgLoad {
    pub role: Option<String>,
    pub before: Vec<Command>,
    pub load_table: String,
    pub after: Vec<Command>,
    pub cols: Vec<ColumnSpec>,
    pub target: String,
}

/// `BEGIN` a transaction and set the role for its scope.
async fn start_txn(conn: &Client, role: Option<&str>) -> Result<(), anyhow::Error> {
    conn.batch_execute("BEGIN").await?;
    if let Some(role) = role {
        anyhow::ensure!(
            !role.contains('"'),
            "Role name cannot contain double quotes: {}",
            role
        );
        conn.execute(&format!(r#"SET LOCAL ROLE "{}""#, role), &[])
            .await?;
    }
    Ok(())
}

/// Execute a resolved command sequence in order, stopping at the first error. Reused for the
/// pre-load and post-load sequences.
async fn run_commands(
    conn: &Client,
    commands: &[Command],
    cols: &[ColumnSpec],
) -> Result<(), anyhow::Error> {
    for cmd in commands {
        match cmd {
            Command::Sql(sql) => execute_sql(conn, sql).await?,
            Command::Prepared { stmt, params } => {
                execute_prepared_stmt(conn, stmt, params, cols).await?
            }
            Command::CreateTable(table) => execute_create_table(conn, table, cols).await?,
            Command::CreateTempTable(table) => execute_create_temp_table(conn, table, cols).await?,
            Command::Merge {
                target,
                source,
                keys,
            } => execute_merge(conn, source, target, keys, cols).await?,
        }
    }
    Ok(())
}

/// The full Postgres sink: a staged load in one transaction, composing [`PgCopySink`] with the
/// pre-/post-load command sequences. Owns the pooled connection from `open` until
/// `commit`/`rollback`.
pub struct PgSink {
    copy: PgCopySink,
    after: Vec<Command>,
    cols: Vec<ColumnSpec>,
    target: String,
}

impl PgSink {
    /// Consume a checked-out connection: `BEGIN`, set the role, run the pre-load commands, and
    /// open the binary COPY. On any setup failure, roll back and drop the connection.
    pub async fn open(conn: Object, load: PgLoad) -> Result<Self, anyhow::Error> {
        let PgLoad {
            role,
            before,
            load_table,
            after,
            cols,
            target,
        } = load;
        let prepared = async {
            start_txn(&conn, role.as_deref()).await?;
            run_commands(&conn, &before, &cols).await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(e) = prepared {
            let _ = conn.batch_execute("ROLLBACK").await;
            return Err(e);
        }

        let copy = PgCopySink::open(conn, &load_table, cols.clone()).await?;
        Ok(PgSink {
            copy,
            after,
            cols,
            target,
        })
    }

    /// The resolved destination identity (the landing `schema.table`).
    pub fn target(&self) -> &str {
        &self.target
    }

    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), anyhow::Error> {
        self.copy.write_batch(batch).await
    }

    /// Finish the COPY, run the post-load commands, and `COMMIT`. A post-load failure rolls back.
    pub async fn commit(self) -> Result<(), anyhow::Error> {
        let PgSink {
            copy, after, cols, ..
        } = self;
        let conn = copy.finish().await?;
        let finalized = run_commands(&conn, &after, &cols).await;
        if let Err(e) = finalized {
            let _ = conn.batch_execute("ROLLBACK").await;
            return Err(e);
        }
        conn.batch_execute("COMMIT").await?;
        Ok(())
    }

    /// Abandon the COPY and roll back the transaction.
    pub async fn rollback(self) {
        let conn = self.copy.cancel();
        let _ = conn.batch_execute("ROLLBACK").await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_copy_sink_is_send() {
        // Spawnable (Send + 'static) is load-bearing for the future parallel-COPY
        fn assert_send<T: Send + 'static>() {}
        assert_send::<PgCopySink>();
    }
}
