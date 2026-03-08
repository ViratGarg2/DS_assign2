package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "github.com/viratgarg/grpc-demo/queue_management/proto"
)

// Track submitted tasks and their last known status
var (
	taskStatusMu   sync.Mutex
	taskLastStatus = make(map[string]string) // task_id -> last known status
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <server_address>")
	}

	serverAddr := os.Args[1]

	// Connect to server
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewTaskQueueServiceClient(conn)

	// Start background status poller
	go pollTaskStatuses(client)

	// Interactive CLI
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Job Client connected. Commands:")
	fmt.Println("  submit <CPU|IO> <payload>  - Submit a task")
	fmt.Println("  status <task_id>           - Check task status")
	fmt.Println("  exit                       - Exit client")
	fmt.Println()

	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading input: %v", err)
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		command := strings.ToLower(parts[0])

		switch command {
		case "submit":
			handleSubmit(client, parts)
		case "status":
			handleStatus(client, parts)
		case "exit":
			fmt.Println("Goodbye!")
			return
		default:
			fmt.Println("Unknown command. Use: submit, status, or exit")
		}
	}
}

func handleSubmit(client pb.TaskQueueServiceClient, parts []string) {
	if len(parts) < 3 {
		fmt.Println("Usage: submit <CPU|IO> <payload>")
		return
	}

	taskType := strings.ToUpper(parts[1])
	payload := parts[2]

	if taskType != "CPU" && taskType != "IO" {
		fmt.Println("Task type must be CPU or IO")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.SubmitTask(ctx, &pb.TaskRequest{
		Type:    taskType,
		Payload: payload,
	})
	if err != nil {
		fmt.Printf("[Client] Error submitting task: %v\n", err)
		return
	}

	taskID := resp.GetTaskId()
	fmt.Printf("[Client] Task submitted. ID: %s (%s, payload=%s)\n", taskID, taskType, payload)

	// Track this task for automatic status polling
	taskStatusMu.Lock()
	taskLastStatus[taskID] = "Queued"
	taskStatusMu.Unlock()
}

func handleStatus(client pb.TaskQueueServiceClient, parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: status <task_id>")
		return
	}

	taskID := parts[1]

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetTaskStatus(ctx, &pb.TaskStatusRequest{
		TaskId: taskID,
	})
	if err != nil {
		fmt.Printf("[Client] Error getting status: %v\n", err)
		return
	}

	if resp.GetWorkerId() != "" {
		fmt.Printf("[Client] Task %s: %s (Worker %s)\n", resp.GetTaskId(), resp.GetStatus(), resp.GetWorkerId())
	} else {
		fmt.Printf("[Client] Task %s: %s\n", resp.GetTaskId(), resp.GetStatus())
	}
}

// pollTaskStatuses periodically checks status of all tracked tasks
// and prints when status changes
func pollTaskStatuses(client pb.TaskQueueServiceClient) {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		taskStatusMu.Lock()
		tasksToCheck := make(map[string]string)
		for taskID, lastStatus := range taskLastStatus {
			tasksToCheck[taskID] = lastStatus
		}
		taskStatusMu.Unlock()

		for taskID, lastStatus := range tasksToCheck {
			// Skip completed or terminal tasks
			if lastStatus == "Completed" {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := client.GetTaskStatus(ctx, &pb.TaskStatusRequest{TaskId: taskID})
			cancel()

			if err != nil {
				continue
			}

			newStatus := resp.GetStatus()
			if newStatus != lastStatus {
				// Status changed, print notification
				if resp.GetWorkerId() != "" {
					fmt.Printf("\n[Client] Task %s: %s (Worker %s)\n> ", taskID, newStatus, resp.GetWorkerId())
				} else {
					fmt.Printf("\n[Client] Task %s: %s\n> ", taskID, newStatus)
				}

				// Update tracked status
				taskStatusMu.Lock()
				taskLastStatus[taskID] = newStatus
				taskStatusMu.Unlock()
			}
		}
	}
}
