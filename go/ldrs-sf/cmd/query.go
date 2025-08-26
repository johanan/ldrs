package cmd

import (
	"errors"
	"net/url"
	"os"

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
			return err
		}

		return nil
	},
}

func init() {
	queryCmd.Flags().String("sql", "", "SQL command to query")
}
