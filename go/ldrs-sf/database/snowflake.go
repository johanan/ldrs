package database

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"net/url"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/snowflakedb/gosnowflake"
)

type SnowflakeConn struct {
	Snowflake *sql.DB
}

func NewSnowflakeConn(connUrl *url.URL) (*SnowflakeConn, error) {
	scheme := connUrl.Scheme
	if scheme != "snowflake" {
		return nil, errors.New("only snowflake scheme is supported")
	}

	privateKey := os.Getenv("LDRS_SF_PEM_KEY")
	key_file := os.Getenv("LDRS_SF_PEM_FILE")

	if privateKey == "" && key_file != "" {
		fileBytes, err := os.ReadFile(key_file)
		if err != nil {
			return nil, err
		}
		privateKey = string(fileBytes)
	}

	if privateKey != "" {
		key, err := ParsePEMPrivateKey(privateKey)
		if err != nil {
			return nil, err
		}
		q := connUrl.Query()
		q.Set("privateKey", GeneratePKCS8StringSupress(key))
		q.Set("authenticator", "SNOWFLAKE_JWT")
		connUrl.RawQuery = q.Encode()
	}

	connString := connUrl.String()
	connString = strings.ReplaceAll(connString, "snowflake://", "")

	db, err := sql.Open("snowflake", connString)
	if err != nil {
		return nil, err
	}

	return &SnowflakeConn{
		Snowflake: db,
	}, nil
}

func (sf *SnowflakeConn) Close() error {
	return sf.Snowflake.Close()
}

func (sf *SnowflakeConn) ExecuteCommands(ctx context.Context, sql_commands []string) ([]string, error) {
	var results []string
	db := sf.Snowflake
	for _, command := range sql_commands {
		stmt, err := db.PrepareContext(gosnowflake.WithHigherPrecision(ctx), command)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
		var status string
		err = stmt.QueryRowContext(ctx, nil).Scan(&status)
		if err != nil {
			return nil, err
		}
		results = append(results, status)
	}
	return results, nil
}

func (sf *SnowflakeConn) ExecuteQuery(ctx context.Context, query string) error {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	sf_ctx := gosnowflake.WithArrowBatchesTimestampOption(
		gosnowflake.WithArrowAllocator(
			gosnowflake.WithArrowBatches(
				gosnowflake.WithHigherPrecision(ctx)), pool), gosnowflake.UseNanosecondTimestamp)

	conn, err := sf.Snowflake.Conn(sf_ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	var rows driver.Rows
	err = conn.Raw(func(x any) error {
		rows, err = x.(driver.QueryerContext).QueryContext(sf_ctx, query, nil)
		return err
	})
	if err != nil {
		return err
	}
	defer rows.Close()

	batches, err := rows.(gosnowflake.SnowflakeRows).GetArrowBatches()
	if err != nil {
		return err
	}

	var ipcWriter *ipc.Writer
	var schema *arrow.Schema

	for i, batch := range batches {
		records, err := batch.WithContext(sf_ctx).Fetch()
		if err != nil {
			return err
		}

		for j, record := range *records {
			if i == 0 && j == 0 {
				schema = record.Schema()
				ipcWriter = ipc.NewWriter(os.Stdout, ipc.WithSchema(schema), ipc.WithAllocator(pool))
				defer ipcWriter.Close()
			}
			if err := ipcWriter.Write(record); err != nil {
				return err
			}
			record.Release()
		}
	}

	return nil
}

func ParsePEMPrivateKey(pemKey string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemKey))
	if block == nil {
		return nil, errors.New("failed to decode PEM block containing RSA private key")
	}

	var privateKey *rsa.PrivateKey

	if block.Type == "RSA PRIVATE KEY" {
		privateKey, _ = x509.ParsePKCS1PrivateKey(block.Bytes)
	} else if block.Type == "PRIVATE KEY" {
		privKey, _ := x509.ParsePKCS8PrivateKey(block.Bytes)
		privateKey = privKey.(*rsa.PrivateKey)
	} else {
		return nil, errors.New("unsupported key type")
	}

	return privateKey, nil
}

func GeneratePKCS8StringSupress(key *rsa.PrivateKey) string {
	// Copied straight from snowflake's go driver
	tmpBytes, _ := x509.MarshalPKCS8PrivateKey(key)
	privKeyPKCS8 := base64.URLEncoding.EncodeToString(tmpBytes)
	return privKeyPKCS8
}
