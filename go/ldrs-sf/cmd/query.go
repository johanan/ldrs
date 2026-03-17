package cmd

import (
	"errors"
	"net/url"
	"os"
	"sort"
	"strings"

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

		parallel, err := cmd.Flags().GetInt("parallel")
		if err != nil {
			return err
		}

		vars := getLdrsSfVars()
		err = sf.ExecuteQuery(ctx, sqlCommand, vars, parallel)
		return err
	},
}

func init() {
	queryCmd.Flags().String("sql", "", "SQL command to query")
	queryCmd.Flags().Int("parallel", 2, "Number of parallel readers to use for the query")
}

func getLdrsSfVars() []any {
	prefix := "LDRS_SF_PARAM_"
	keys := make([]string, 0)
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, prefix) {
			// just get the key for now
			parts := strings.SplitN(env, "=", 2)
			if len(parts) > 0 {
				keys = append(keys, parts[0])
			}
		}
	}
	// sort the keys
	sort.Strings(keys)
	values := make([]any, len(keys))
	for i, key := range keys {
		values[i] = os.Getenv(key)
	}
	return values
}
