package main

import (
	"cabinet/config"
	"cabinet/mongodb"
	"encoding/gob"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type ServerDock struct {
	serverID   int
	addr       string
	txClient   *rpc.Client
	prioClient *rpc.Client
	jobQMu     sync.RWMutex
	jobQ       map[prioClock]chan struct{}
}

// conns does not store the operating servers' information
var conns = struct {
	sync.RWMutex
	m map[int]*ServerDock
}{
	m: make(map[int]*ServerDock),
}

func runFollower() {
	serverConfig := config.ParseClusterConfig(numOfServers, configPath)
	ipIndex := config.ServerIP
	rpcPortIndex := config.ServerRPCListenerPort

	myAddr := serverConfig[myServerID][ipIndex] + ":" + serverConfig[myServerID][rpcPortIndex]
	log.Debugf("config: serverID %d | addr: %s", myServerID, myAddr)

	// initialize follower
	go mongoDBCleanUp()
	initMongoDB()

	err := rpc.Register(NewCabService())
	if err != nil {
		log.Fatalf("rp.Reister failed | error: %v", err)
		return
	}

	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		log.Fatalf("ListenTCP error: %v", err)
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
			return
		}

		go rpc.ServeConn(conn)
	}
}

func initMongoDB() {
	gob.Register([]mongodb.Query{})

	if mode == Localhost {
		mongoDbFollower = mongodb.NewMongoFollower(mongoClientNum, int(1), myServerID)
	} else {
		mongoDbFollower = mongodb.NewMongoFollower(mongoClientNum, int(1), 0)
	}

	println(mongodb.DataPath)
	queriesToLoad, err := mongodb.ReadQueryFromFile(mongodb.DataPath)
	if err != nil {
		log.Errorf("getting load data failed | error: %v", err)
		return
	}

	err = mongoDbFollower.ClearTable("tasks")
	if err != nil {
		log.Errorf("clean up table failed | err: %v", err)
		return
	}

	log.Debugf("loading data to Mongo DB")
	_, _, err = mongoDbFollower.FollowerAPI(queriesToLoad)
	if err != nil {
		log.Errorf("load data failed | error: %v", err)
		return
	}

	log.Infof("mongo DB initialization done")
}

// mongoDBCleanUp cleans up client connections to DB upon ctrl+C
func mongoDBCleanUp() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Debugf("clean up MongoDb follower")
		err := mongoDbFollower.CleanUp()
		if err != nil {
			log.Errorf("clean up MongoDB follower failed | err: %v", err)
			return
		}
		log.Infof("clean up MongoDB follower succeeded")
		os.Exit(1)
	}()
}
