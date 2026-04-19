package database

import (
	"bufio"
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
	"sync"

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

func (sf *SnowflakeConn) ExecuteQuery(ctx context.Context, query string, args []any, numReaders int) error {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	sf_ctx := gosnowflake.WithArrowBatchesTimestampOption(
		gosnowflake.WithArrowAllocator(
			gosnowflake.WithArrowBatches(
				gosnowflake.WithHigherPrecision(ctx)), pool), gosnowflake.UseMillisecondTimestamp)

	conn, err := sf.Snowflake.Conn(sf_ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	namedArgs := toNamedValues(args)
	var rows driver.Rows
	err = conn.Raw(func(x any) error {
		rows, err = x.(driver.QueryerContext).QueryContext(sf_ctx, query, namedArgs)
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

	type batchResult struct {
		index   int
		records *[]arrow.Record
	}

	batchCh := make(chan int, numReaders)
	resultCh := make(chan batchResult, numReaders)
	errorCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(sf_ctx)
	defer cancel()

	// waitgroups
	var readersWG sync.WaitGroup
	var writerWG sync.WaitGroup

	writerWG.Add(1)
	go func() {
		defer writerWG.Done()

		buffer := make(map[int]*[]arrow.Record)
		nextIndex := 0

		// wait for the first batch to arrive to get the schema, but buffer any batches that arrive in the meantime
		for result := range resultCh {
			buffer[result.index] = result.records
			if _, has := buffer[0]; has {
				break
			}
		}

		if buffer[0] == nil {
			// write an empty ipc stream and exit
			// no rows so no schema and nothing to do, but we did not error
			writeEmptyStream(pool)
			return
		}

		schema := (*buffer[0])[0].Schema()
		bufWriter := bufio.NewWriter(os.Stdout)
		ipcWriter := ipc.NewWriter(bufWriter, ipc.WithSchema(schema), ipc.WithAllocator(pool))
		defer func() {
			ipcWriter.Close()
			bufWriter.Flush()
		}()

		writeRecords := func(recs *[]arrow.Record) error {
			for _, record := range *recs {
				if err := ipcWriter.Write(record); err != nil {
					return err
				}
				record.Release()
			}
			return nil
		}

		for {
			select {
			case records, ok := <-resultCh:
				if !ok {
					for {
						recs, exists := buffer[nextIndex]
						if !exists {
							break
						}
						delete(buffer, nextIndex)
						if err := writeRecords(recs); err != nil {
							bufWriter.Flush()
							select {
							case errorCh <- err:
							default:
							}
							return
						}
						nextIndex++
					}
					return
				}
				buffer[records.index] = records.records

				for {
					recs, exists := buffer[nextIndex]
					if !exists {
						break
					}
					delete(buffer, nextIndex)
					if err := writeRecords(recs); err != nil {
						bufWriter.Flush()
						select {
						case errorCh <- err:
						default:
						}
						return
					}
					nextIndex++
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	readerWorker := func() {
		defer readersWG.Done()

		for {
			select {
			case i, ok := <-batchCh:
				if !ok {
					return
				}

				records, err := batches[i].WithContext(sf_ctx).Fetch()
				if err != nil {
					select {
					case errorCh <- err:
					default:
					}
					return
				}

				select {
				case resultCh <- batchResult{index: i, records: records}:
				case <-ctx.Done():
					return
				}

			case <-ctx.Done():
				return
			}
		}
	}

	readersWG.Add(numReaders)
	for range numReaders {
		go readerWorker()
	}

	go func() {
		defer close(batchCh)
		for i := range batches {
			select {
			case batchCh <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		readersWG.Wait()
		close(resultCh)
	}()

	writerWG.Wait()

	select {
	case err := <-errorCh:
		cancel()
		return err
	default:
		return nil
	}
}

func writeEmptyStream(pool memory.Allocator) {
	emptySchema := arrow.NewSchema([]arrow.Field{}, nil)
	bufWriter := bufio.NewWriter(os.Stdout)
	ipcWriter := ipc.NewWriter(bufWriter, ipc.WithSchema(emptySchema), ipc.WithAllocator(pool))
	ipcWriter.Close()
	bufWriter.Flush()
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

func toNamedValues(values []any) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(values))
	for idx, value := range values {
		namedValues[idx] = driver.NamedValue{Name: "", Ordinal: idx + 1, Value: value}
	}
	return namedValues
}
