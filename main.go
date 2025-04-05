package main

import (
	"cabinet/eval"
	"cabinet/mongodb"
	"cabinet/smr"
	"fmt"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()
var mypriority = smr.NewServerPriority(-1, 0)
var mystate = smr.NewServerState()
var pManager smr.PriorityManager
var perfM eval.PerfMeter

// Type aliases.
type serverID = int
type prioClock = int
type priority = float64

var nextNodeIndex = 0

// MongoDB instance variables
var mongoDbLeader *mongodb.MongoLeader
var mongoDbFollower *mongodb.MongoFollower

func init() {
	fmt.Println("program starts ...")
	loadCommandLineInputs()
	setLogger(logLevel)

	mystate.SetMyServerID(myServerID)
	mystate.SetLeaderID(0)

	pManager.Init(numOfServers, quorum, 1, ratioTryStep, enablePriority)
	fileName := fmt.Sprintf("s%d_n%d_f%d_b%d_%s", myServerID, numOfServers, quorum, batchsize, suffix)
	perfM.Init(1, batchsize, fileName)
}

func main() {
	mypriority.Majority = pManager.GetMajority()
	pscheme := pManager.GetPriorityScheme()

	fmt.Println("information board")
	fmt.Printf("priority scheme: %v\n", pscheme)
	fmt.Printf("majority: %v\n", mypriority.Majority)

	if myServerID == 0 {
		// Leader initialization: create a dedicated MongoDB instance.
		mongoDbLeader = mongodb.NewMongoLeader(0) // Using db "tasks" (or "tasks0")
		if mongoDbLeader == nil {
			log.Fatal("Failed to initialize MongoDB Leader instance")
		}
		mypriority.PrioVal = pscheme[0]
		establishRPCs()
		log.Infof("establishRPCs() was successful.")
		startSyncCabInstance()
	} else {
		// Follower initialization: create a local MongoDB instance.
		runFollower()
	}
}
