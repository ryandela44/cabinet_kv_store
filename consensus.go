package main

import (
	"cabinet/config"
	pb "cabinet/gpu-scheduler/proto/go"
	"cabinet/mongodb"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// Global task queue (mutex-protected slice) and its mutex.
var (
	taskList      []mongodb.Query
	taskListMutex sync.Mutex
)

// pushTask appends a task to the task queue.
func pushTask(task mongodb.Query) {
	taskListMutex.Lock()
	taskList = append(taskList, task)
	taskListMutex.Unlock()
}

// popTask removes and returns the first task in the queue.
// It returns false if no task is available.
func popTask() (mongodb.Query, bool) {
	taskListMutex.Lock()
	defer taskListMutex.Unlock()
	if len(taskList) == 0 {
		return mongodb.Query{}, false
	}
	task := taskList[0]
	taskList = taskList[1:]
	return task, true
}

// loadTasksContinuously repeatedly reads the CSV file and appends tasks.
func loadTasksContinuously() {
	for {
		queries, err := mongodb.TailReadQueryFromFile(mongodb.DataPath)
		if err != nil {
			log.Errorf("Error reading task file: %v", err)
		} else {
			for _, q := range queries {
				pushTask(q)
			}
		}
		time.Sleep(1 * time.Second) // adjust as needed
	}
}

func startSyncCabInstance() {
	leaderPClock := 0

	// Prepare crash list if needed.
	crashList := prepCrashList()
	log.Infof("Crash list was successfully prepared: %v", crashList)

	var possibleTs chan int
	if dynamicT {
		possibleTs = config.ParseThresholds("./config/possibleTs.conf")
	}

	// Start the continuous task loader.
	go loadTasksContinuously()

	// Main consensus loop: process tasks from the queue.
	for {
		// Wait until a task is available.
		var task mongodb.Query
		for {
			var ok bool
			task, ok = popTask()
			if ok {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		serviceMethod := "CabService.ConsensusService"
		receiver := make(chan ReplyInfo, numOfServers)

		// Crash tests (if configured)
		if leaderPClock == crashTime && crashMode != 0 {
			conns.Lock()
			for _, sID := range crashList {
				delete(conns.m, sID)
			}
			conns.Unlock()
		}

		startTime := time.Now()

		// 1. Get Priority.
		fpriorities := pManager.GetFollowerPriorities(leaderPClock)
		log.Infof("pClock: %v | priorities: %+v", leaderPClock, fpriorities)
		log.Infof("pClock: %v | quorum size (t+1) is %v | majority: %v",
			leaderPClock, pManager.GetQuorumSize(), mypriority.Majority)

		if dynamicT {
			q := <-possibleTs
			fpriorities = pManager.SetNewPrioritiesUnderNewT(numOfServers, q+1, 1, ratioTryStep, leaderPClock)
			log.Infof("pClock: %v | NEW priorities: %+v", leaderPClock, fpriorities)
			log.Infof("pClock: %v | NEW quorum size (t+1) is %v | majority: %v",
				leaderPClock, pManager.GetQuorumSize(), mypriority.Majority)
		}

		// 2. Assignment: choose node via round-robin.
		selectedNode := getNextNode(numOfServers)
		task.Values["assigned_board"] = strconv.Itoa(selectedNode)
		assignQuery := createAssignmentUpdateQuery(task, selectedNode)
		_, _, err := mongoDbLeader.LeaderAPI(assignQuery)
		if err != nil {
			log.Errorf("Assignment update failed for task Key=%v: %v", task.Key, err)
			continue
		}

		perfM.RecordStarter(leaderPClock)

		// 3. Issue consensus RPC calls.
		if issueMongoDBOps(leaderPClock, fpriorities, serviceMethod, receiver, task) {
			if err := perfM.SaveToFile(); err != nil {
				log.Errorf("perfM save to file failed: %v", err)
			}
			return
		}

		// 4. Wait for consensus RPC responses.
		prioSum := mypriority.PrioVal
		prioQueue := make(chan serverID, numOfServers)
		var followersResults []ReplyInfo
		for rinfo := range receiver {
			prioQueue <- rinfo.SID
			log.Infof("Received RPC response: pClock: %v | serverID: %v", leaderPClock, rinfo.SID)
			fpriorities = pManager.GetFollowerPriorities(leaderPClock)
			prioSum += fpriorities[rinfo.SID]
			followersResults = append(followersResults, rinfo)
			if prioSum > mypriority.Majority {
				if err := perfM.RecordFinisher(leaderPClock); err != nil {
					log.Errorf("PerfMeter failed: %v", err)
					return
				}
				mystate.AddCommitIndex(batchsize)
				log.Infof("Consensus reached | pClock: %v | elapsed: %v ms | commitIndex: %v",
					leaderPClock, time.Since(startTime).Milliseconds(), mystate.GetCommitIndex())
				break
			}
		}

		// 5. Task Execution: send task to scheduler via gRPC.
		taskId, err := strconv.Atoi(task.Key)
		if err != nil {
			log.Errorf("Invalid TaskID for task Key=%v: %v", task.Key, err)
			continue
		}
		assignedNode, err := strconv.Atoi(task.Values["assigned_board"])
		if err != nil {
			log.Errorf("Invalid assigned_board for task Key=%v: %v", task.Key, err)
			continue
		}
		schedulerAddress := getSchedulerAddress(assignedNode)
		reply, err := sendTaskToScheduler(schedulerAddress, task)
		if err != nil {
			log.Errorf("sendTaskToScheduler failed for task Key=%v: %v", task.Key, err)
			continue
		}
		log.Infof("Task %v (TaskID=%v) executed with reply: %v", task.Key, taskId, reply)

		// 6. Finalization: mark task as completed.
		completionTime := time.Now().Format(time.RFC3339)
		updatedQuery := createFinalizationUpdateQuery(task, completionTime)
		_, _, err = mongoDbLeader.LeaderAPI(updatedQuery)
		if err != nil {
			log.Errorf("Finalization update failed: %v", err)
		}

		leaderPClock++
		err = pManager.UpdateFollowerPriorities(leaderPClock, prioQueue, mystate.GetLeaderID())
		if err != nil {
			log.Errorf("UpdateFollowerPriorities failed: %v", err)
			return
		}
		log.Infof("Priority updated for pClock %v", leaderPClock)
	}
}

func issueMongoDBOps(pClock prioClock, p map[serverID]priority, method string, r chan ReplyInfo, query mongodb.Query) (allDone bool) {
	conns.RLock()
	defer conns.RUnlock()

	for _, conn := range conns.m {
		args := &Args{
			PrioClock: pClock,
			PrioVal:   p[conn.serverID],
			CmdMongo:  query,
		}
		go executeRPC(conn, method, args, r)
	}
	return
}

func prepCrashList() (crashList []int) {
	switch crashMode {
	case 0:
		// no crash
	case 1:
		for i := 1; i < quorum; i++ {
			crashList = append(crashList, i)
		}
	case 2:
		for i := 1; i < quorum; i++ {
			crashList = append(crashList, numOfServers-i)
		}
	case 3:
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
	}
	return crashList
}

func executeRPC(conn *ServerDock, serviceMethod string, args *Args, receiver chan ReplyInfo) {
	reply := Reply{}
	stack := make(chan struct{}, 1)

	conn.jobQMu.Lock()
	conn.jobQ[args.PrioClock] = stack
	conn.jobQMu.Unlock()

	if args.PrioClock > 0 {
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

func getNextNode(numOfServers int) int {
	selected := nextNodeIndex
	nextNodeIndex++
	if nextNodeIndex >= (numOfServers) {
		nextNodeIndex = 0
	}
	return selected
}

func createAssignmentUpdateQuery(task mongodb.Query, selectedNode int) mongodb.Query {
	updateQuery := mongodb.Query{
		Op:    mongodb.UPDATE,
		Table: task.Table,
		Key:   task.Key,
		Values: map[string]string{
			"assigned_board": strconv.Itoa(selectedNode),
		},
	}
	return updateQuery
}

func createFinalizationUpdateQuery(task mongodb.Query, completionTime string) mongodb.Query {
	updateQuery := mongodb.Query{
		Op:    mongodb.UPDATE,
		Table: task.Table,
		Key:   task.Key,
		Values: map[string]string{
			"status":          "2", // Completed
			"completion_time": completionTime,
		},
	}
	return updateQuery
}

func sendTaskToScheduler(selectedNode string, task mongodb.Query) (string, error) {
	conn, err := grpc.Dial(selectedNode, grpc.WithInsecure())
	if err != nil {
		return "", fmt.Errorf("failed to dial scheduler: %v", err)
	}
	defer conn.Close()

	client := pb.NewSchedulerClient(conn)

	taskId, err := strconv.Atoi(task.Key)
	if err != nil {
		return "", fmt.Errorf("invalid TaskID: %v", err)
	}

	assignedBoard, err := strconv.Atoi(task.Values["assigned_board"])
	if err != nil {
		return "", fmt.Errorf("invalid AssignedBoard: %v", err)
	}
	status, err := strconv.Atoi(task.Values["status"])
	if err != nil {
		return "", fmt.Errorf("invalid Status: %v", err)
	}
	deadline, err := strconv.Atoi(task.Values["deadline"])
	if err != nil {
		return "", fmt.Errorf("invalid Deadline: %v", err)
	}

	grpcTask := &pb.TaskStatus{
		TaskId:         int32(taskId),
		AssignedBoard:  int32(assignedBoard),
		Status:         int32(status),
		SubmitTime:     task.Values["submit_time"],
		StartTime:      task.Values["start_time"],
		GpuReq:         task.Values["gpu_req"],
		Deadline:       int32(deadline),
		CompletionTime: task.Values["completion_time"],
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := client.ExecuteTask(ctx, grpcTask)
	if err != nil {
		return "", err
	}
	return reply.Reply, nil
}

func getSchedulerAddress(nodeID int) string {
	switch nodeID {
	case 1:
		return fmt.Sprintf("127.0.0.1:11001")
	case 2:
		return fmt.Sprintf("127.0.0.1:11002")
	}
	return fmt.Sprintf("127.0.0.1:11000")
}
