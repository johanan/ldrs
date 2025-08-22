package database

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"net/url"
	"os"
	"strings"

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
