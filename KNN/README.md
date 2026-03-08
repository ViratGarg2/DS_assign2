# KNN gRPC Service

This project implements a **K-Nearest Neighbors (KNN)** system using **gRPC in Go**.
The server loads a dataset from a CSV file and exposes RPC methods that allow clients to query the KNN model.

---

## Prerequisites

Make sure the following tools are installed:

* **Go** (1.18+ recommended)
* **protoc (Protocol Buffers compiler)**
* Go protobuf plugins:

  ```bash
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  ```

Ensure the binaries are available in your `PATH`.

---

## Project Structure

```
.
├── proto/
│   └── hello.proto
├── server/
│   └── main.go
├── client/
│   └── main.go
├── data/
│   └── <csv files>
```

* **proto/** – Protocol buffer definitions for the service
* **server/** – gRPC server implementation
* **client/** – gRPC client implementation
* **data/** – CSV datasets used by the KNN service

---

## Step 1: Generate gRPC Code

Navigate to the **KNN project folder** and run:

```bash
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/hello.proto
```

This generates the required **Go gRPC bindings** from the `.proto` file.

---

## Step 2: Run the Server

Start one or more server instances:

```bash
go run server/main.go <port_number>
```

Example:

```bash
go run server/main.go 50051
```

---

## Step 3: Run the Client

Start the client and provide a **list of server ports**:

```bash
go run client/main.go <list_of_port_numbers>
```

Example:

```bash
go run client/main.go 50051 50052 50053
```

---

## Client Usage

Once the client starts, you will be prompted to choose one of the available options.

### 1. Load Dataset

* Choose the **dataset loading option** from the client menu.
* Ensure the **CSV file is present inside the `/data` directory** before loading.

---

### 2. Run KNN Query

To perform a KNN query:

1. Enter the **input vector**

   * Values should be **comma-separated**.

   Example:

   ```
   5.1,3.5,1.4,0.2
   ```

2. Enter the value of **k** (number of neighbors).

3. Press **Enter** to run the query.

The client will send the request to the server(s) and return the predicted result.

---

## Notes

* Ensure all servers are running before starting the client.
* The dataset must be loaded before performing KNN queries.
* The CSV file must exist in the **`/data` directory**.

---

## Example Workflow

```bash
# Generate gRPC code
protoc ...

# Start servers
go run server/main.go 50051
go run server/main.go 50052

# Start client
go run client/main.go 50051 50052
```

Then use the client interface to **load the dataset and perform KNN queries**.

---
