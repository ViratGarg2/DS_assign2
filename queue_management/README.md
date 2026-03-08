# Distributed Task Queue System (Go + gRPC)

This project implements a **distributed task queue system** using **gRPC in Go**.
The system consists of three components:

* **Server** – Manages the task queue and distributes jobs to workers.
* **Workers** – Execute tasks (CPU-bound, IO-bound, or both).
* **Client** – Submits tasks and checks their status.

---

# Prerequisites

Ensure the following tools are installed:

* **Go** (1.18 or later)
* **Protocol Buffers (`protoc`)**
* Go protobuf plugins

Install the required protobuf plugins:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Make sure `$GOPATH/bin` or `$HOME/go/bin` is in your `PATH`.

---

# Project Structure

```
.
├── proto/
│   └── task_queue.proto
├── server/
│   └── main.go
├── worker/
│   └── main.go
├── client/
│   └── main.go
```

* **proto/** – gRPC service definitions
* **server/** – Task queue manager
* **worker/** – Workers that process tasks
* **client/** – Interface for submitting and tracking jobs

---

# Step 1: Generate gRPC Code

From the **project root directory**, run:

```bash
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/task_queue.proto
```

This generates the Go files required for the gRPC service.

---

# Step 2: Start the Server

Run the task queue server:

```bash
go run server/main.go localhost:2002
```

The server listens for **worker registrations and client requests** on port **2002**.

---

# Step 3: Start Workers

Workers connect to the server and execute tasks.

```bash
go run worker/main.go localhost:2002 <CPU | IO | BOTH>
```

Worker types:

| Type | Description               |
| ---- | ------------------------- |
| CPU  | Processes CPU-bound tasks |
| IO   | Processes IO-bound tasks  |
| BOTH | Can process both types    |

Example:

```bash
go run worker/main.go localhost:2002 CPU
```

You can run **multiple workers simultaneously** to increase throughput.

---

# Step 4: Start the Client

Run the client:

```bash
go run client/main.go localhost:2002
```

The client connects to the server and allows users to **submit tasks and query their status**.

---

# Client Commands

Once the client starts, the following commands are available:

### Submit a Job

```
submit <CPU | IO> <payload>
```

Example:

```
submit CPU compute_matrix
```

This submits a task of the specified type with the given payload.

---

### Check Task Status

```
status <Task_ID>
```

Example:

```
status 42
```

The server returns the **current state of the task** (queued, running, completed, etc.).

---

### Exit the Client

```
exit
```

This terminates the client session.

---

# Example Workflow

Start the server:

```bash
go run server/main.go localhost:2002
```

Start workers:

```bash
go run worker/main.go localhost:2002 CPU
go run worker/main.go localhost:2002 IO
```

Start the client:

```bash
go run client/main.go localhost:2002
```

Submit a task:

```
submit CPU heavy_computation
```

Check its status:

```
status 1
```

Exit the client:

```
exit
```

---

# Notes

* Multiple workers can be launched to handle tasks concurrently.
* Workers automatically register with the server when started.
* Tasks are dispatched to workers based on their supported task type.

---
