package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "github.com/viratgarg/grpc-demo/queue_management/proto"
)

var workerID string

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: go run main.go <server_address> <worker_type>\n  worker_type: CPU, IO, or BOTH")
	}

	serverAddr := os.Args[1]
	workerType := os.Args[2]

	// Validate worker type
	if workerType != "CPU" && workerType != "IO" && workerType != "BOTH" {
		log.Fatal("worker_type must be CPU, IO, or BOTH")
	}

	// Connect to server
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewTaskQueueServiceClient(conn)

	// Register worker
	ctx := context.Background()
	regResp, err := client.RegisterWorker(ctx, &pb.WorkerConfig{WorkerType: workerType})
	if err != nil {
		log.Fatalf("Failed to register worker: %v", err)
	}
	workerID = regResp.GetWorkerId()
	fmt.Printf("[Worker] Registered with ID: %s (type: %s)\n", workerID, workerType)

	// Start heartbeat goroutine
	go startHeartbeat(client)

	// Request tasks (streaming)
	fmt.Printf("[Worker] Waiting for tasks...\n")
	requestTasks(client)
}

func startHeartbeat(client pb.TaskQueueServiceClient) {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := client.SendHeartbeat(ctx, &pb.Heartbeat{WorkerId: workerID})
		cancel()
		if err != nil {
			log.Printf("[Worker %s] Failed to send heartbeat: %v", workerID, err)
		}
	}
}

func requestTasks(client pb.TaskQueueServiceClient) {
	ctx := context.Background()
	stream, err := client.RequestTask(ctx, &pb.WorkerIdRequest{WorkerId: workerID})
	if err != nil {
		log.Fatalf("[Worker %s] Failed to request tasks: %v", workerID, err)
	}

	for {
		task, err := stream.Recv()
		if err == io.EOF {
			fmt.Printf("[Worker %s] Stream closed by server\n", workerID)
			return
		}
		if err != nil {
			log.Printf("[Worker %s] Error receiving task: %v", workerID, err)
			return
		}

		// Process the task
		processTask(client, task)
	}
}

func processTask(client pb.TaskQueueServiceClient, task *pb.Task) {
	fmt.Printf("[Worker %s] Received task %s (%s, payload=%s)\n", workerID, task.TaskId, task.Type, task.Payload)

	// Update status to Processing
	ctx := context.Background()
	_, err := client.UpdateTaskStatus(ctx, &pb.TaskStatusUpdate{
		WorkerId: workerID,
		TaskId:   task.TaskId,
		Status:   "Processing",
	})
	if err != nil {
		log.Printf("[Worker %s] Failed to update status to Processing: %v", workerID, err)
	}

	// Perform the task
	switch task.Type {
	case "CPU":
		performCPUTask(task.Payload)
	case "IO":
		performIOTask(task.Payload)
	}

	// Update status to Completed
	_, err = client.UpdateTaskStatus(ctx, &pb.TaskStatusUpdate{
		WorkerId: workerID,
		TaskId:   task.TaskId,
		Status:   "Completed",
	})
	if err != nil {
		log.Printf("[Worker %s] Failed to update status to Completed: %v", workerID, err)
	}

	fmt.Printf("[Worker %s] Task %s completed. Waiting for next task...\n", workerID, task.TaskId)
}

func performCPUTask(payload string) {
	n, err := strconv.Atoi(payload)
	if err != nil {
		log.Printf("[Worker %s] Invalid CPU payload: %s", workerID, payload)
		return
	}

	fmt.Printf("[Worker %s] Performing %dx%d matrix multiplication...\n", workerID, n, n)

	// Create and multiply two n×n matrices
	matrixA := make([][]float64, n)
	matrixB := make([][]float64, n)
	result := make([][]float64, n)

	for i := 0; i < n; i++ {
		matrixA[i] = make([]float64, n)
		matrixB[i] = make([]float64, n)
		result[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			matrixA[i][j] = rand.Float64()
			matrixB[i][j] = rand.Float64()
		}
	}

	// Matrix multiplication
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			sum := 0.0
			for k := 0; k < n; k++ {
				sum += matrixA[i][k] * matrixB[k][j]
			}
			result[i][j] = sum
		}
	}
}

func performIOTask(payload string) {
	secs, err := strconv.Atoi(payload)
	if err != nil {
		log.Printf("[Worker %s] Invalid IO payload: %s", workerID, payload)
		return
	}

	fmt.Printf("[Worker %s] Sleeping for %d seconds (simulating I/O)...\n", workerID, secs)
	time.Sleep(time.Duration(secs) * time.Second)
}
