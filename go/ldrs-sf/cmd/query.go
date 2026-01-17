package cmd

import (
	"errors"
	"net/url"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/johanan/ldrs/go/ldrs-sf/database"
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Query data from Snowflake",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		sqlCommand, err := cmd.Flags().GetString("sql")
		if err != nil {
			return err
		}

		srcString := os.Getenv("LDRS_SF_SOURCE")
		if srcString == "" {
			return errors.New("LDRS_SF_SOURCE environment variable is not set")
		}

		src, err := url.Parse(srcString)
		if err != nil {
			return err
		}

		sf, err := database.NewSnowflakeConn(src)
		if err != nil {
			return err
		}
		defer sf.Close()

		err = sf.ExecuteQuery(ctx, sqlCommand)
		if err != nil {
			// write out an empty arrow schema and file
			schema := arrow.NewSchema([]arrow.Field{}, nil)
			alloc := memory.NewGoAllocator()
			writer := ipc.NewWriter(os.Stdout, ipc.WithSchema(schema), ipc.WithAllocator(alloc))

			recordBuilder := array.NewRecordBuilder(alloc, schema)
			emptyBatch := recordBuilder.NewRecord()
			defer recordBuilder.Release()
			defer emptyBatch.Release()
			if err := writer.Write(emptyBatch); err != nil {
				return err
			}
			if err := writer.Close(); err != nil {
				return err
			}
			return err
		}

		return nil
	},
}

func init() {
	queryCmd.Flags().String("sql", "", "SQL command to query")
}
