package cmd

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"

	"github.com/johanan/ldrs/go/ldrs-sf/database"
	"github.com/spf13/cobra"
)

var execCmd = &cobra.Command{
	Use:   "exec",
	Short: "Execute one or more SQL statements against Snowflake",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		sqlCommands, err := cmd.Flags().GetStringArray("sql")
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

		output, err := sf.ExecuteCommands(ctx, sqlCommands)
		if err != nil {
			return err
		}

		// json encode the per-statement result sets
		jsonOutput, err := json.Marshal(output)
		if err != nil {
			return err
		}

		os.Stdout.Write(jsonOutput)

		return nil
	},
}

func init() {
	execCmd.Flags().StringArray("sql", nil, "SQL statement to execute (repeatable)")
}
