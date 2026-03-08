package main

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	pb "github.com/viratgarg/grpc-demo/queue_management/proto"
)

// ==================== Data Structures ====================

type TaskInfo struct {
	ID        string
	Type      string    // "CPU" or "IO"
	Payload   string
	Status    string    // "Queued", "Processing", "Completed", "Failed"
	WorkerID  string    // assigned worker (empty if queued)
	CreatedAt time.Time
}

type WorkerInfo struct {
	ID            string
	Type          string    // "CPU", "IO", "BOTH"
	LastHeartbeat time.Time
	CurrentTask   string    // task_id being processed (empty if idle)
	IsAlive       bool
	TaskChan      chan *pb.Task // channel to push tasks to this worker
}

type server struct {
	pb.UnimplementedTaskQueueServiceServer
	mu sync.Mutex

	// Task storage
	tasks    map[string]*TaskInfo // task_id -> TaskInfo
	cpuQueue *list.List           // FIFO queue of task_ids (CPU tasks)
	ioQueue  *list.List           // FIFO queue of task_ids (IO tasks)

	// Worker management
	workers map[string]*WorkerInfo // worker_id -> WorkerInfo

	// ID counters
	taskCounter   int
	workerCounter int
}

func newServer() *server {
	return &server{
		tasks:    make(map[string]*TaskInfo),
		cpuQueue: list.New(),
		ioQueue:  list.New(),
		workers:  make(map[string]*WorkerInfo),
	}
}

// ==================== Job Client RPCs ====================

func (s *server) SubmitTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate task ID
	s.taskCounter++
	taskID := fmt.Sprintf("T%d", s.taskCounter)

	// Create task info
	task := &TaskInfo{
		ID:        taskID,
		Type:      req.GetType(),
		Payload:   req.GetPayload(),
		Status:    "Queued",
		WorkerID:  "",
		CreatedAt: time.Now(),
	}
	s.tasks[taskID] = task

	// Add to appropriate queue
	if req.GetType() == "CPU" {
		s.cpuQueue.PushBack(taskID)
	} else {
		s.ioQueue.PushBack(taskID)
	}

	log.Printf("[Server] TASK ARRIVAL: Task %s (%s, payload=%s) added to queue", taskID, req.GetType(), req.GetPayload())

	// Try to assign to an idle worker
	s.tryAssignTasksLocked()

	return &pb.TaskResponse{TaskId: taskID}, nil
}

func (s *server) GetTaskStatus(ctx context.Context, req *pb.TaskStatusRequest) (*pb.TaskStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[req.GetTaskId()]
	if !exists {
		return &pb.TaskStatusResponse{
			TaskId:   req.GetTaskId(),
			Status:   "Not Found",
			WorkerId: "",
		}, nil
	}

	workerInfo := ""
	if task.WorkerID != "" {
		workerInfo = task.WorkerID
	}

	return &pb.TaskStatusResponse{
		TaskId:   task.ID,
		Status:   task.Status,
		WorkerId: workerInfo,
	}, nil
}

// ==================== Worker RPCs ====================

func (s *server) RegisterWorker(ctx context.Context, req *pb.WorkerConfig) (*pb.WorkerIdResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate worker ID
	s.workerCounter++
	workerID := fmt.Sprintf("W%d", s.workerCounter)

	// Create worker info
	worker := &WorkerInfo{
		ID:            workerID,
		Type:          req.GetWorkerType(),
		LastHeartbeat: time.Now(),
		CurrentTask:   "",
		IsAlive:       true,
		TaskChan:      make(chan *pb.Task, 1),
	}
	s.workers[workerID] = worker

	log.Printf("[Server] Worker registered. ID: %s (type: %s)", workerID, req.GetWorkerType())

	return &pb.WorkerIdResponse{WorkerId: workerID}, nil
}

func (s *server) RequestTask(req *pb.WorkerIdRequest, stream pb.TaskQueueService_RequestTaskServer) error {
	workerID := req.GetWorkerId()

	s.mu.Lock()
	worker, exists := s.workers[workerID]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("worker %s not found", workerID)
	}
	taskChan := worker.TaskChan
	s.mu.Unlock()

	log.Printf("[Server] Worker %s waiting for tasks...", workerID)

	// Try to assign any pending tasks to this worker
	s.mu.Lock()
	s.tryAssignTasksLocked()
	s.mu.Unlock()

	// Stream tasks to worker
	for {
		select {
		case task, ok := <-taskChan:
			if !ok {
				// Channel closed, worker marked dead
				log.Printf("[Server] Worker %s stream closed", workerID)
				return nil
			}
			if err := stream.Send(task); err != nil {
				log.Printf("[Server] Error sending task to worker %s: %v", workerID, err)
				return err
			}
		case <-stream.Context().Done():
			log.Printf("[Server] Worker %s disconnected", workerID)
			return stream.Context().Err()
		}
	}
}

func (s *server) SendHeartbeat(ctx context.Context, req *pb.Heartbeat) (*pb.Acknowledge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, exists := s.workers[req.GetWorkerId()]
	if !exists {
		return &pb.Acknowledge{Success: false, Message: "Worker not found"}, nil
	}

	worker.LastHeartbeat = time.Now()
	worker.IsAlive = true

	return &pb.Acknowledge{Success: true, Message: "Heartbeat received"}, nil
}

func (s *server) UpdateTaskStatus(ctx context.Context, req *pb.TaskStatusUpdate) (*pb.Acknowledge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	workerID := req.GetWorkerId()
	taskID := req.GetTaskId()
	status := req.GetStatus()

	task, exists := s.tasks[taskID]
	if !exists {
		return &pb.Acknowledge{Success: false, Message: "Task not found"}, nil
	}

	worker, workerExists := s.workers[workerID]
	if !workerExists {
		return &pb.Acknowledge{Success: false, Message: "Worker not found"}, nil
	}

	// Update task status
	task.Status = status

	if status == "Processing" {
		log.Printf("[Server] Task %s: Processing (Worker %s)", taskID, workerID)
	} else if status == "Completed" {
		log.Printf("[Server] Task %s: Completed", taskID)
		// Clear worker's current task
		worker.CurrentTask = ""
		// Try to assign next task
		s.tryAssignTasksLocked()
	} else if status == "Failed" {
		log.Printf("[Server] Task %s: Failed", taskID)
		// Clear worker's current task
		worker.CurrentTask = ""
		// Re-queue at front
		s.requeueTaskAtFrontLocked(task)
		// Try to assign
		s.tryAssignTasksLocked()
	}

	return &pb.Acknowledge{Success: true, Message: "Status updated"}, nil
}

// ==================== Task Assignment Logic ====================

// Must be called with s.mu held
func (s *server) tryAssignTasksLocked() {
	for _, worker := range s.workers {
		if !worker.IsAlive || worker.CurrentTask != "" {
			continue // Worker is dead or busy
		}

		var task *TaskInfo
		var taskElement *list.Element

		switch worker.Type {
		case "CPU":
			task, taskElement = s.popMatchingTaskLocked(s.cpuQueue)
		case "IO":
			task, taskElement = s.popMatchingTaskLocked(s.ioQueue)
		case "BOTH":
			task, taskElement = s.popEarliestTaskLocked()
		}

		if task != nil {
			// Remove from queue
			if taskElement != nil {
				if task.Type == "CPU" {
					s.cpuQueue.Remove(taskElement)
				} else {
					s.ioQueue.Remove(taskElement)
				}
			}

			// Assign task to worker
			task.Status = "Processing"
			task.WorkerID = worker.ID
			worker.CurrentTask = task.ID

			// Push task to worker's channel
			pbTask := &pb.Task{
				TaskId:  task.ID,
				Type:    task.Type,
				Payload: task.Payload,
			}

			select {
			case worker.TaskChan <- pbTask:
				log.Printf("[Server] TASK ASSIGNMENT: Task %s assigned to Worker %s", task.ID, worker.ID)
			default:
				// Channel full, worker might be busy
				log.Printf("[Server] Could not assign task %s to worker %s (channel full)", task.ID, worker.ID)
				// Revert assignment
				task.Status = "Queued"
				task.WorkerID = ""
				worker.CurrentTask = ""
				// Re-add to front of queue
				if task.Type == "CPU" {
					s.cpuQueue.PushFront(task.ID)
				} else {
					s.ioQueue.PushFront(task.ID)
				}
			}
		}
	}
}

// Must be called with s.mu held
func (s *server) popMatchingTaskLocked(queue *list.List) (*TaskInfo, *list.Element) {
	if queue.Len() == 0 {
		return nil, nil
	}
	elem := queue.Front()
	taskID := elem.Value.(string)
	task := s.tasks[taskID]
	return task, elem
}

// Must be called with s.mu held - for BOTH workers, pick earliest task
func (s *server) popEarliestTaskLocked() (*TaskInfo, *list.Element) {
	var earliestTask *TaskInfo
	var earliestElem *list.Element

	if s.cpuQueue.Len() > 0 {
		elem := s.cpuQueue.Front()
		taskID := elem.Value.(string)
		task := s.tasks[taskID]
		earliestTask = task
		earliestElem = elem
	}

	if s.ioQueue.Len() > 0 {
		elem := s.ioQueue.Front()
		taskID := elem.Value.(string)
		task := s.tasks[taskID]
		if earliestTask == nil || task.CreatedAt.Before(earliestTask.CreatedAt) {
			earliestTask = task
			earliestElem = elem
		}
	}

	return earliestTask, earliestElem
}

// Must be called with s.mu held
func (s *server) requeueTaskAtFrontLocked(task *TaskInfo) {
	task.Status = "Queued"
	task.WorkerID = ""
	if task.Type == "CPU" {
		s.cpuQueue.PushFront(task.ID)
	} else {
		s.ioQueue.PushFront(task.ID)
	}
	log.Printf("[Server] Task %s re-queued at front", task.ID)
}

// ==================== Heartbeat Checker ====================

func (s *server) startHeartbeatChecker() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			s.checkHeartbeats()
		}
	}()
}

func (s *server) checkHeartbeats() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, worker := range s.workers {
		if !worker.IsAlive {
			continue
		}

		// Check if worker missed 3 consecutive heartbeats (15 seconds)
		if now.Sub(worker.LastHeartbeat) > 15*time.Second {
			log.Printf("[Server] Worker %s missed heartbeats, marking as dead", worker.ID)
			s.markWorkerDeadLocked(worker)
		}
	}
}

// Must be called with s.mu held
func (s *server) markWorkerDeadLocked(worker *WorkerInfo) {
	worker.IsAlive = false
	log.Printf("[Server] FAILURE DETECTED: Worker %s is dead (missed 3 heartbeats)", worker.ID)

	// If worker was processing a task, mark it as Failed and re-queue
	if worker.CurrentTask != "" {
		task, exists := s.tasks[worker.CurrentTask]
		if exists {
			log.Printf("[Server] Task %s: Failed (worker %s died)", task.ID, worker.ID)
			task.Status = "Failed"
			// Re-queue at front
			s.requeueTaskAtFrontLocked(task)
		}
		worker.CurrentTask = ""
	}

	// Close worker's task channel to terminate streaming RPC
	close(worker.TaskChan)

	// Try to assign re-queued task to other workers
	s.tryAssignTasksLocked()
}

// ==================== Main ====================

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <address:port>")
	}

	address := os.Args[1]

	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	srv := newServer()
	pb.RegisterTaskQueueServiceServer(grpcServer, srv)

	// Start heartbeat checker
	srv.startHeartbeatChecker()

	log.Printf("[Server] Listening on %s", address)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
