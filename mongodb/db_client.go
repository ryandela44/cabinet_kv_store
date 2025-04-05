package mongodb

import (
	"context"
	"encoding/csv"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"strconv"
)

// Query represents a MongoDB command.
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

const DataPath string = "mongodb/task.csv"

// queryHandlerSingle processes a single query using a given mongo.Database.
func queryHandlerSingle(db *mongo.Database, query Query) (result map[string]string, err error) {
	switch query.Op {
	case INSERT:
		if err = dbInsert(db, query.Table, query.Key, query.Values); err != nil {
			return nil, err
		}
		return map[string]string{"INSERT": "done"}, nil
	case READ:
		results, err := dbRead(db, query.Table, query.Key, query.Values)
		if err != nil {
			return nil, err
		}
		if len(results) > 0 {
			return results[0], nil
		}
		return map[string]string{"READ": "done"}, nil
	case UPDATE:
		if err = dbUpdate(db, query.Table, query.Key, query.Values); err != nil {
			return nil, err
		}
		return map[string]string{"UPDATE": "done"}, nil
	case SCAN:
		scanCount, ok := query.Values["<all fields>"]
		if !ok || scanCount == "" {
			return nil, fmt.Errorf("unexpected scan count")
		}
		recordCount, err := strconv.Atoi(scanCount)
		if err != nil {
			return nil, err
		}
		results, err := dbScan(db, query.Table, query.Key, recordCount, query.Values)
		if err != nil {
			return nil, err
		}
		if len(results) > 0 {
			return results[0], nil
		}
		return map[string]string{"SCAN": "done"}, nil
	case DELETE:
		if err := dbDelete(db, query.Table, query.Key); err != nil {
			return nil, err
		}
		return map[string]string{"DELETE": "done"}, nil
	case DROP:
		if err := dbDrop(db, query.Table); err != nil {
			return nil, err
		}
		return map[string]string{"DROP": "done"}, nil
	default:
		return nil, fmt.Errorf("unexpected operator: %d", query.Op)
	}
}

// --- The following functions (dbInsert, dbRead, etc.) are largely unchanged ---

func dbInsert(db *mongo.Database, table string, key string, values map[string]string) error {
	coll := db.Collection(table)
	insValues := bson.D{{Key: "_id", Value: key}}
	for f, v := range values {
		insValues = append(insValues, bson.E{Key: f, Value: v})
	}
	_, err := coll.InsertOne(context.TODO(), insValues)
	return err
}

func dbRead(db *mongo.Database, table string, key string, fields map[string]string) ([]map[string]string, error) {
	coll := db.Collection(table)
	filter := bson.D{}
	if key != "" {
		filter = bson.D{{Key: "_id", Value: key}}
	}
	cursor, err := coll.Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}
	results, err := fillMap(cursor)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func dbUpdate(db *mongo.Database, table string, key string, values map[string]string) error {
	coll := db.Collection(table)
	filter := bson.D{{Key: "_id", Value: key}}
	var updateValues bson.D
	for f, v := range values {
		updateValues = append(updateValues, bson.E{Key: f, Value: v})
	}
	_, err := coll.UpdateMany(context.TODO(), filter, bson.D{{Key: "$set", Value: updateValues}})
	return err
}

func dbScan(db *mongo.Database, table string, startkey string, recordcount int, fields map[string]string) ([]map[string]string, error) {
	coll := db.Collection(table)
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: startkey}}}}
	sort := bson.D{{Key: "_id", Value: 1}}
	cursor, err := coll.Find(context.TODO(), filter, options.Find().SetSort(sort), options.Find().SetLimit(int64(recordcount)))
	if err != nil {
		return nil, err
	}
	results, err := fillMap(cursor)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func dbDelete(db *mongo.Database, table string, key string) error {
	coll := db.Collection(table)
	filter := bson.D{{Key: "_id", Value: key}}
	if key == "" {
		filter = bson.D{}
	}
	_, err := coll.DeleteMany(context.TODO(), filter)
	return err
}

func dbDrop(db *mongo.Database, table string) error {
	coll := db.Collection(table)
	return coll.Drop(context.TODO())
}

func fillMap(cursor *mongo.Cursor) ([]map[string]string, error) {
	var results []bson.M
	if err := cursor.All(context.TODO(), &results); err != nil {
		return nil, err
	}
	var resultMaps []map[string]string
	for _, res := range results {
		m := make(map[string]string)
		for k, v := range res {
			if str, ok := v.(string); ok {
				m[k] = str
			} else {
				m[k] = fmt.Sprintf("%v", v)
			}
		}
		resultMaps = append(resultMaps, m)
	}
	return resultMaps, nil
}

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

// Global variable to track the last processed task_id.
var lastTaskID int = 0

// TailReadQueryFromFile reads the entire CSV file and returns only tasks with task_id > lastTaskID.
func TailReadQueryFromFile(fn string) ([]Query, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var queries []Query
	for i, record := range records {
		if i == 0 {
			continue // skip header row
		}
		var query Query
		if err := csvLineToQuery(record, &query); err != nil {
			// Optionally log and skip
			continue
		}
		// Convert task_id (stored in query.Key) to an integer.
		taskID, err := strconv.Atoi(query.Key)
		if err != nil {
			continue
		}
		// Only add new tasks with a taskID greater than lastTaskID.
		if taskID > lastTaskID {
			queries = append(queries, query)
			// Update lastTaskID.
			if taskID > lastTaskID {
				lastTaskID = taskID
			}
		}
	}
	return queries, nil
}
