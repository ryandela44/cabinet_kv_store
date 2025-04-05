package mongodb

import (
	"context"
	"os"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoLeader is used by the leader to manage tasks.
type MongoLeader struct {
	client *mongo.Client
	dbName string
}

// NewMongoLeader creates a new leader MongoDB instance.
// dbID is used to choose a database name (e.g., "tasks" or "tasks1", etc.).
func NewMongoLeader(dbID int) *MongoLeader {
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017/"
	}
	cli, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		log.Errorf("Failed to connect to MongoDB: %v", err)
		return nil
	}
	dbName := "tasks"
	if dbID != 0 {
		dbName = "tasks" + strconv.Itoa(dbID)
	}
	return &MongoLeader{
		client: cli,
		dbName: dbName,
	}
}

// LeaderAPI executes a single query against the leader's database.
func (ml *MongoLeader) LeaderAPI(query Query) (result map[string]string, latency time.Duration, err error) {
	db := ml.client.Database(ml.dbName)
	start := time.Now()
	res, err := queryHandlerSingle(db, query)
	if err != nil {
		return nil, 0, err
	}
	latency = time.Since(start)
	return res, latency, nil
}

// ClearTable drops the specified table.
func (ml *MongoLeader) ClearTable(table string) error {
	dropQuery := Query{
		Op:     DROP,
		Table:  table,
		Key:    "",
		Values: nil,
	}
	_, _, err := ml.LeaderAPI(dropQuery)
	if err != nil {
		log.Errorf("ClearTable failed: %v", err)
		return err
	}
	log.Debugf("Table %s cleared", table)
	return nil
}

// CleanUp disconnects the MongoDB client.
func (ml *MongoLeader) CleanUp() error {
	if err := ml.ClearTable("tasks"); err != nil {
		log.Errorf("CleanUp table failed: %v", err)
		return err
	}
	return ml.client.Disconnect(context.TODO())
}
