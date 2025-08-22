package cmd

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"

	"github.com/johanan/ldrs/go/ldrs-sf/database"
	"github.com/spf13/cobra"
)

var execCmd = &cobra.Command{
	Use:   "exec",
	Short: "Execute a SQL command against Snowflake",
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

		sqlCommands := strings.Split(sqlCommand, ";")
		nonEmptyCommands := make([]string, 0, len(sqlCommands))

		for _, sql := range sqlCommands {
			if sql != "" {
				nonEmptyCommands = append(nonEmptyCommands, sql)
			}
		}

		output, err := sf.ExecuteCommands(ctx, nonEmptyCommands)
		if err != nil {
			return err
		}

		// json encode the string array of outcomes
		jsonOutput, err := json.Marshal(output)
		if err != nil {
			return err
		}

		os.Stdout.Write(jsonOutput)

		return nil
	},
}

func init() {
	execCmd.Flags().String("sql", "", "SQL command to execute")
}
