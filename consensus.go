package main

import (
	"cabinet/config"
	"cabinet/mongodb"
	"math/rand"
	"time"
)

func startSyncCabInstance() {
	leaderPClock := 0
	var mongoDBQueries []mongodb.Query

	var err error
	mongoDBQueries, err = mongodb.ReadQueryFromFile(mongodb.DataPath)
	if err != nil {
		log.Errorf("ReadQueryFromFile failed | err: %v", err)
		return
	}

	// prepare crash list
	crashList := prepCrashList()
	log.Infof("crash list was successfully prepared.")

	var possibleTs chan int
	// get possible thresholds, which is stored in a channel
	if dynamicT {
		possibleTs = config.ParseThresholds("./config/possibleTs.conf")
	}

	for {

		serviceMethod := "CabService.ConsensusService"

		receiver := make(chan ReplyInfo, numOfServers)

		// crash tests
		if leaderPClock == crashTime && crashMode != 0 {
			conns.Lock()
			for _, sID := range crashList {
				delete(conns.m, sID)
			}
			conns.Unlock()
		}

		startTime := time.Now()

		// 1. get priority
		fpriorities := pManager.GetFollowerPriorities(leaderPClock)
		log.Infof("pClock: %v | priorities: %+v", leaderPClock, fpriorities)
		log.Infof("pClock: %v | quorum size (t+1) is %v | majority: %v", leaderPClock, pManager.GetQuorumSize(), pManager.GetMajority())

		log.Debugf("Testing priorities change under new thresholds")

		// implementing dynamically changing thresholds
		if dynamicT {
			q := <-possibleTs
			fpriorities = pManager.SetNewPrioritiesUnderNewT(numOfServers, q+1, 1, ratioTryStep, leaderPClock)

			log.Infof("pClock: %v | NEW priorities: %+v", leaderPClock, fpriorities)
			log.Infof("pClock: %v | NEW quorum size (t+1) is %v | majority: %v", leaderPClock, pManager.GetQuorumSize(), pManager.GetMajority())
		}

		// 2. broadcast rpcs
		perfM.RecordStarter(leaderPClock)

		if issueMongoDBOps(leaderPClock, fpriorities, serviceMethod, receiver, mongoDBQueries) {
			if err := perfM.SaveToFile(); err != nil {
				log.Errorf("perfM save to file failed | err: %v", err)
			}
			return
		}

		// 3. waiting for results
		prioSum := mypriority.PrioVal
		prioQueue := make(chan serverID, numOfServers)
		var followersResults []ReplyInfo

		for rinfo := range receiver {

			prioQueue <- rinfo.SID
			log.Infof("recv pClock: %v | serverID: %v", leaderPClock, rinfo.SID)

			fpriorities := pManager.GetFollowerPriorities(leaderPClock)

			prioSum += fpriorities[rinfo.SID]

			followersResults = append(followersResults, rinfo)

			if prioSum > mypriority.Majority {
				if err := perfM.RecordFinisher(leaderPClock); err != nil {
					log.Errorf("PerfMeter failed | err: %v", err)
					return
				}

				mystate.AddCommitIndex(batchsize)

				log.Infof("consensus reached | insID: %v | total time elapsed: %v | cmtIndex: %v",
					leaderPClock, time.Now().Sub(startTime).Milliseconds(), mystate.GetCommitIndex())
				break
			}
		}

		leaderPClock++
		err := pManager.UpdateFollowerPriorities(leaderPClock, prioQueue, mystate.GetLeaderID())
		if err != nil {
			log.Errorf("UpdateFollowerPriorities failed | err: %v", err)
			return
		}
		log.Infof("prio updated for pClock %v", leaderPClock)
	}
}

func issueMongoDBOps(pClock prioClock, p map[serverID]priority, method string, r chan ReplyInfo, allQueries []mongodb.Query) (allDone bool) {
	conns.RLock()
	defer conns.RUnlock()

	left := pClock * batchsize
	right := (pClock+1)*batchsize - 1
	if right > len(allQueries) {
		log.Infof("MongoDB evaluation finished")
		allDone = true
		return
	}

	for _, conn := range conns.m {
		args := &Args{
			PrioClock: pClock,
			PrioVal:   p[conn.serverID],
			CmdMongo:  allQueries[left:right],
		}

		go executeRPC(conn, method, args, r)
	}

	return
}

func prepCrashList() (crashList []int) {
	switch crashMode {
	case 0:
		break
	case 1:
		for i := 1; i < quorum; i++ {
			crashList = append(crashList, i)
		}
	case 2:
		for i := 1; i < quorum; i++ {
			crashList = append(crashList, numOfServers-i)
		}
	case 3:
		// // evenly distributed
		// for i := 0; i < 5; i++ {
		// 	for j := 1; j <= (quorum-1) / 5; j++ {
		// 		crashList = append(crashList, i*(numOfServers/5) + j)
		// 	}
		// }

		// randomly distributed
		rand.Seed(time.Now().UnixNano())

		for i := 1; i < quorum; i++ {
			contains := false
			for {
				crashID := rand.Intn(numOfServers-1) + 1

				for _, cID := range crashList {
					if cID == crashID {
						contains = true
						break
					}
				}

				if contains {
					contains = false
					continue
				} else {
					crashList = append(crashList, crashID)
					break
				}
			}
		}
	default:
		break
	}

	return
}

func executeRPC(conn *ServerDock, serviceMethod string, args *Args, receiver chan ReplyInfo) {
	reply := Reply{}

	stack := make(chan struct{}, 1)

	conn.jobQMu.Lock()
	conn.jobQ[args.PrioClock] = stack
	conn.jobQMu.Unlock()

	if args.PrioClock > 0 {
		// Waiting for the completion of its previous RPC
		<-conn.jobQ[args.PrioClock-1]
	}

	err := conn.txClient.Call(serviceMethod, args, &reply)

	if err != nil {
		log.Errorf("RPC call error: %v", err)
		return
	}

	rinfo := ReplyInfo{
		SID:    conn.serverID,
		PClock: args.PrioClock,
		Recv:   reply,
	}
	receiver <- rinfo

	conn.jobQMu.Lock()
	conn.jobQ[args.PrioClock] <- struct{}{}
	conn.jobQMu.Unlock()

	log.Debugf("RPC %s succeeded | result: %+v", serviceMethod, rinfo)
}
