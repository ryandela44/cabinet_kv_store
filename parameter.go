package main

import (
	"flag"
)

const (
	Localhost = iota
	Distributed
)

var numOfServers int
var threshold int
var quorum int
var myServerID int
var configPath string
var production bool
var logLevel string
var mode int

var batchsize int
var msgsize int

var ratioTryStep float64
var enablePriority bool

var dynamicT bool

// Mongo DB input parameters
var mongoLoadType string
var mongoClientNum int

// crash test parameters
var crashTime int
var crashMode int

// suffix of files
var suffix string

func loadCommandLineInputs() {
	flag.IntVar(&numOfServers, "n", 3, "# of servers")

	flag.IntVar(&threshold, "t", 1, "# of quorum tolerated")
	// f = t + 1;
	quorum = threshold + 1

	flag.IntVar(&batchsize, "b", 1, "batch size")
	flag.IntVar(&myServerID, "id", 0, "this server ID")
	flag.StringVar(&configPath, "path", "./config/cluster_localhost.conf", "config file path")
	flag.BoolVar(&dynamicT, "dt", false, "changing Ts?")

	flag.StringVar(&logLevel, "log", "debug", "trace, debug, info, warn, error, fatal, panic")
	flag.IntVar(&mode, "mode", 0, "0 -> localhost; 1 -> distributed")

	flag.Float64Var(&ratioTryStep, "rstep", 0.001, "rate for trying qualified ratio")

	flag.BoolVar(&enablePriority, "ep", false, "true -> cabinet; false -> raft")

	// Plain message input parameters
	flag.IntVar(&msgsize, "ms", 512, "message size")

	// MongoDB input parameters
	flag.IntVar(&mongoClientNum, "mcli", 16, "# of mongodb clients")

	// crash test parameters
	flag.IntVar(&crashTime, "ct", 20, "# of rounds before crash")
	flag.IntVar(&crashMode, "cm", 0, "0 -> no crash; 1 -> strong machines; 2 -> weak machines; 3 -> random machines")

	// suffix of files
	flag.StringVar(&suffix, "suffix", "xxx", "suffix of files")

	flag.Parse()

	log.Debugf("CommandLine parameters:\n - numOfServers:%v\n - myServerID:%v\n", numOfServers, myServerID)
}
