use std::pin::Pin;

use anyhow::Context;
use arrow_array::RecordBatch;
use deadpool_postgres::Object;
use ldrs_arrow::{ColumnSpec, TypedColumnAccessor};
use tokio_postgres::binary_copy::BinaryCopyInWriter;

use crate::extracted_values::{ColumnConverter, ExtractedValue};
use crate::map_colspec_to_pg_type;

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
