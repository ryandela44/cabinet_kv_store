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

// ServerDock stores information for an active RPC connection.
type ServerDock struct {
	serverID   int
	addr       string
	txClient   *rpc.Client
	prioClient *rpc.Client
	jobQMu     sync.RWMutex
	jobQ       map[prioClock]chan struct{}
}

// conns holds the active connections for the cluster.
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

	// Initialize follower's local MongoDB instance.
	go mongoDBCleanUp()
	initMongoDB()

	// Register RPC service.
	err := rpc.Register(NewCabService())
	if err != nil {
		log.Fatalf("rpc.Register failed: %v", err)
		return
	}

	// Start listening for incoming RPC connections.
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
	// Register the type for gob encoding.
	gob.Register([]mongodb.Query{})

	// Use the new MongoFollower constructor.
	// Here, we use myServerID so that each follower maintains its own task state.
	mongoDbFollower = mongodb.NewMongoFollower(myServerID)
	if mongoDbFollower == nil {
		log.Fatalf("Failed to initialize MongoDB follower instance")
	}

	// Optionally clear the local "tasks" table to start fresh.
	err := mongoDbFollower.ClearTable("tasks")
	if err != nil {
		log.Errorf("Clean up table failed: %v", err)
		return
	}

	log.Infof("MongoDB follower initialization done")
}

func mongoDBCleanUp() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Debugf("Cleaning up MongoDB follower")
		err := mongoDbFollower.CleanUp()
		if err != nil {
			log.Errorf("Clean up MongoDB follower failed: %v", err)
			return
		}
		log.Infof("MongoDB follower cleanup succeeded")
		os.Exit(1)
	}()
}
