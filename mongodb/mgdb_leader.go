package mongodb

import (
	"encoding/csv"
	"fmt"
	"os"
)

type Query struct {
	Op     int
	Table  string
	Key    string
	Values map[string]string
}

const (
	INSERT = iota + 3
	READ
	UPDATE
	SCAN
	DELETE
	DROP
)

func csvLineToQuery(record []string, query *Query) error {
	if len(record) != 10 {
		return fmt.Errorf("expected 10 fields in CSV record, got %d", len(record))
	}

	switch record[0] {
	case "INSERT":
		query.Op = INSERT
	case "READ":
		query.Op = READ
	case "UPDATE":
		query.Op = UPDATE
	case "SCAN":
		query.Op = SCAN
	case "DELETE":
		query.Op = DELETE
	default:
		return fmt.Errorf("Unexpected operator: %s\n", record[0])
	}

	query.Table = record[1]
	query.Key = record[2]

	query.Values = map[string]string{
		"status":          record[3],
		"assigned_board":  record[4],
		"gpu_req":         record[5],
		"submit_time":     record[6],
		"start_time":      record[7],
		"completion_time": record[8],
		"deadline":        record[9],
	}

	return nil
}

func ReadQueryFromFile(fn string) (queries []Query, err error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	// Skip header; process subsequent records.
	for i, record := range records {
		if i == 0 {
			continue // skip header row
		}
		var query Query
		if err = csvLineToQuery(record, &query); err != nil {
			return nil, err
		}
		queries = append(queries, query)
	}

	return queries, nil
}
