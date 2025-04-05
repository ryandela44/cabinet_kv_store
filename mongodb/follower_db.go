package mongodb

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var log = logrus.New()

// MongoFollower is used by follower nodes.
type MongoFollower struct {
	client *mongo.Client
	dbName string
}

// NewMongoFollower creates a new follower MongoDB instance.
func NewMongoFollower(dbID int) *MongoFollower {
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
	return &MongoFollower{
		client: cli,
		dbName: dbName,
	}
}

// FollowerAPI executes a single query against the follower's database.
func (mf *MongoFollower) FollowerAPI(query Query) (result map[string]string, latency time.Duration, err error) {
	db := mf.client.Database(mf.dbName)
	start := time.Now()
	res, err := queryHandlerSingle(db, query)
	if err != nil {
		return nil, 0, err
	}
	latency = time.Since(start)
	return res, latency, nil
}

// ClearTable drops the specified table.
func (mf *MongoFollower) ClearTable(table string) error {
	dropQuery := Query{
		Op:     DROP,
		Table:  table,
		Key:    "",
		Values: nil,
	}
	_, _, err := mf.FollowerAPI(dropQuery)
	if err != nil {
		log.Errorf("ClearTable failed: %v", err)
		return err
	}
	log.Debugf("Table %s cleared", table)
	return nil
}

// CleanUp disconnects the MongoDB client.
func (mf *MongoFollower) CleanUp() error {
	if err := mf.ClearTable("tasks"); err != nil {
		return err
	}
	return mf.client.Disconnect(context.TODO())
}
