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
	"strings"
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
		time.Sleep(5 * time.Millisecond)
	}
}

// isDuplicateKeyError returns true if the error's text indicates a duplicate key error.
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	// Customize this check based on your MongoDB driver error text.
	return strings.Contains(err.Error(), "duplicate key")
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
		// --- Consensus Round 1: Assignment Update ---
		startTime := time.Now()

		// Get follower priorities.
		fpriorities := pManager.GetFollowerPriorities(leaderPClock)
		log.Infof("Round1 (Assignment) - pClock: %v | priorities: %+v", leaderPClock, fpriorities)
		log.Infof("Round1 - pClock: %v | quorum size (t+1) is %v | majority: %v",
			leaderPClock, pManager.GetQuorumSize(), mypriority.Majority)
		if dynamicT {
			q := <-possibleTs
			fpriorities = pManager.SetNewPrioritiesUnderNewT(numOfServers, q+1, 1, ratioTryStep, leaderPClock)
			log.Infof("Round1 - pClock: %v | NEW priorities: %+v", leaderPClock, fpriorities)
		}

		// Assignment: choose node via round-robin.
		selectedNode := getNextNode(numOfServers)
		task.Values["assigned_board"] = strconv.Itoa(selectedNode)
		assignQuery := createAssignmentUpdateQuery(task, selectedNode)
		// Update the leader’s DB.
		_, _, err := mongoDbLeader.LeaderAPI(assignQuery)
		if err != nil && !isDuplicateKeyError(err) {
			log.Errorf("Round1: Assignment update failed for task Key=%v: %v", task.Key, err)
			continue
		} else if isDuplicateKeyError(err) {
			log.Warnf("Round1: Duplicate key error for task Key=%v; ignoring", task.Key)
		}
		perfM.RecordStarter(leaderPClock)

		// Propagate the assignment update via consensus.
		receiver := make(chan ReplyInfo, numOfServers)
		if issueMongoDBOps(leaderPClock, fpriorities, serviceMethod, receiver, assignQuery) {
			if err := perfM.SaveToFile(); err != nil {
				log.Errorf("Round1: perfM save to file failed: %v", err)
			}
			return
		}

		// Wait for consensus responses.
		prioSum := mypriority.PrioVal
		prioQueue := make(chan serverID, numOfServers)
		for rinfo := range receiver {
			prioQueue <- rinfo.SID
			log.Infof("Round1: Received RPC response: pClock: %v | serverID: %v", leaderPClock, rinfo.SID)
			fpriorities = pManager.GetFollowerPriorities(leaderPClock)
			prioSum += fpriorities[rinfo.SID]
			if prioSum > mypriority.Majority {
				if err := perfM.RecordFinisher(leaderPClock); err != nil {
					log.Errorf("Round1: PerfMeter failed: %v", err)
					return
				}
				mystate.AddCommitIndex(batchsize)
				log.Infof("Round1: Consensus reached | pClock: %v | elapsed: %v ms | commitIndex: %v",
					leaderPClock, time.Since(startTime).Milliseconds(), mystate.GetCommitIndex())
				break
			}
		}
		leaderPClock++
		err = pManager.UpdateFollowerPriorities(leaderPClock, prioQueue, mystate.GetLeaderID())
		if err != nil {
			log.Errorf("Round1: UpdateFollowerPriorities failed: %v", err)
			return
		}
		log.Infof("Round1: Priority updated for pClock %v", leaderPClock)

		// --- Consensus Round 2: Start Time Update ---
		startTime = time.Now()
		// Record the dispatch time.
		startTimeStr := time.Now().Format(time.RFC3339)
		task.Values["start_time"] = startTimeStr
		startUpdateQuery := createStartTimeUpdateQuery(task, startTimeStr)
		// Update leader’s DB.
		_, _, err = mongoDbLeader.LeaderAPI(startUpdateQuery)
		if err != nil && !isDuplicateKeyError(err) {
			log.Errorf("Round2: Start time update failed: %v", err)
			continue
		} else if isDuplicateKeyError(err) {
			log.Warnf("Round2: Duplicate key error on start time update for task Key=%v; ignoring", task.Key)
		}

		// Propagate the start time update via consensus.
		receiver = make(chan ReplyInfo, numOfServers)
		if issueMongoDBOps(leaderPClock, fpriorities, serviceMethod, receiver, startUpdateQuery) {
			// Handle error if needed.
		}
		prioSum = mypriority.PrioVal
		prioQueue = make(chan serverID, numOfServers)
		for rinfo := range receiver {
			prioQueue <- rinfo.SID
			log.Infof("Round2: Received RPC response: pClock: %v | serverID: %v", leaderPClock, rinfo.SID)
			fpriorities = pManager.GetFollowerPriorities(leaderPClock)
			prioSum += fpriorities[rinfo.SID]
			if prioSum > mypriority.Majority {
				log.Infof("Round2: Consensus reached for start time update | pClock: %v | elapsed: %v ms",
					leaderPClock, time.Since(startTime).Milliseconds())
				break
			}
		}
		leaderPClock++
		err = pManager.UpdateFollowerPriorities(leaderPClock, prioQueue, mystate.GetLeaderID())
		if err != nil {
			log.Errorf("Round2: UpdateFollowerPriorities failed: %v", err)
			return
		}
		log.Infof("Round2: Priority updated for pClock %v", leaderPClock)

		// --- Dispatch Task: Send via gRPC ---
		taskId, err := strconv.Atoi(task.Key)
		if err != nil {
			log.Errorf("Task Execution: Invalid TaskID for task Key=%v: %v", task.Key, err)
			continue
		}
		assignedBoard, err := strconv.Atoi(task.Values["assigned_board"])
		if err != nil {
			log.Errorf("Task Execution: Invalid assigned_board for task Key=%v: %v", task.Key, err)
			continue
		}
		schedulerAddress := getSchedulerAddress(assignedBoard)
		execReply, err := sendTaskToScheduler(schedulerAddress, task)
		if err != nil {
			log.Errorf("Task Execution: sendTaskToScheduler failed for task Key=%v: %v", task.Key, err)
			continue
		}
		log.Infof("Task %v (TaskID=%v) executed with reply: %v", task.Key, taskId, execReply)

		// --- Consensus Round 3: Finalization Update ---
		startTime = time.Now()
		completionTime := time.Now().Format(time.RFC3339)
		// It is a good idea to update the task values as well.
		task.Values["completion_time"] = completionTime
		finalQuery := createFinalizationUpdateQuery(task, completionTime)
		// Update leader’s DB.
		_, _, err = mongoDbLeader.LeaderAPI(finalQuery)
		if err != nil && !isDuplicateKeyError(err) {
			log.Errorf("Round3: Finalization update failed: %v", err)
		} else if isDuplicateKeyError(err) {
			log.Warnf("Round3: Duplicate key error on finalization update for task Key=%v; ignoring", task.Key)
		}
		// Propagate finalization via consensus.
		receiver = make(chan ReplyInfo, numOfServers)
		if issueMongoDBOps(leaderPClock, fpriorities, serviceMethod, receiver, finalQuery) {
			// Handle error if needed.
		}
		prioSum = mypriority.PrioVal
		prioQueue = make(chan serverID, numOfServers)
		for rinfo := range receiver {
			prioQueue <- rinfo.SID
			log.Infof("Round3: Received RPC response: pClock: %v | serverID: %v", leaderPClock, rinfo.SID)
			fpriorities = pManager.GetFollowerPriorities(leaderPClock)
			prioSum += fpriorities[rinfo.SID]
			if prioSum > mypriority.Majority {
				log.Infof("Round3: Consensus reached for finalization update | pClock: %v | elapsed: %v ms",
					leaderPClock, time.Since(startTime).Milliseconds())
				break
			}
		}
		leaderPClock++
		err = pManager.UpdateFollowerPriorities(leaderPClock, prioQueue, mystate.GetLeaderID())
		if err != nil {
			log.Errorf("Round3: UpdateFollowerPriorities failed: %v", err)
			return
		}
		log.Infof("Round3: Priority updated for pClock %v", leaderPClock)
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

func createStartTimeUpdateQuery(task mongodb.Query, startTime string) mongodb.Query {
	updateQuery := mongodb.Query{
		Op:    mongodb.UPDATE,
		Table: task.Table,
		Key:   task.Key,
		Values: map[string]string{
			"start_time": startTime,
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

	log.Infof("Sending TaskStatus: task_id=%d, assigned_board=%d, status=%d, submit_time=%s, gpu_req=%s, deadline=%d, completion_time=%s",
		int32(taskId), int32(assignedBoard), int32(status), task.Values["submit_time"], task.Values["gpu_req"], int32(deadline), task.Values["completion_time"])

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
