// service.go
package main

import (
	"cabinet/mongodb"
	"errors"
	"time"
)

type CabService struct{}

func NewCabService() *CabService {
	return &CabService{}
}

type Args struct {
	PrioClock int
	PrioVal   float64
	CmdMongo  mongodb.Query
}

type ReplyInfo struct {
	SID    int
	PClock int
	Recv   Reply
}

type Reply struct {
	ServerID  int
	PrioClock int
	ExeResult string
	ErrorMsg  error
}

func (s *CabService) ConsensusService(args *Args, reply *Reply) error {
	// 1. First update priority
	log.Infof("received args: %v", args)
	err := mypriority.UpdatePriority(args.PrioClock, args.PrioVal)
	if err != nil {
		log.Errorf("update priority failed | err: %v", err)
		reply.ErrorMsg = err
		return err
	}

	// 2. Then do transaction job
	return conJobMongoDB(args, reply)

	err = errors.New("unidentified job")
	log.Errorf("err: %v | receievd type: %v", err)
	return err
}

func conJobMongoDB(args *Args, reply *Reply) (err error) {
	log.Debugf("Server %d is executing PClock %d", myServerID, args.PrioClock)

	start := time.Now()

	_, queryLatency, err := mongoDbFollower.FollowerAPI(args.CmdMongo)
	if err != nil {
		log.Errorf("run cmd failed | err: %v | queryLatency %v", err, queryLatency)
		reply.ErrorMsg = err
		return
	}

	reply.ExeResult = time.Since(start).String()

	// //fmt.Println("Average latency of Mongo DB queries: ", queryLatency)
	// for i, queryRes := range queryResults {
	// 	if i >= 2 && i < len(queryResults)-3 {
	// 		continue
	// 	}
	// 	//fmt.Printf("\nResult of the %vth query: \n", i)
	// 	for _, queRes := range queryRes {
	// 		//if uid, ok := queRes["_id"]; ok {
	// 		if _, ok := queRes["_id"]; ok {
	// 			//fmt.Println("_id", "is", uid)
	// 			delete(queRes, "_id")
	// 		}
	// 		//for k, v := range queRes {
	// 		//	fmt.Println(k, "is", v)
	// 		//}
	// 	}
	// }

	log.Debugf("Server %d finished PClock %d", myServerID, args.PrioClock)

	return
}
