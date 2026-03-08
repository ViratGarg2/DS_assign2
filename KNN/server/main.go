package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"math"
	"google.golang.org/grpc"
	pb "github.com/viratgarg/grpc-demo/proto"
	"sort"
)

type server struct {
	pb.UnimplementedGreeterServer
	mu            sync.Mutex
	storedVectors []*pb.Vector
}

// ------------------- StoreBatch -------------------

func (s *server) StoreBatch(ctx context.Context, req *pb.VectorBatch) (*pb.StoreResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Replace entire stored data
	s.storedVectors = make([]*pb.Vector, len(req.GetVectors()))
	copy(s.storedVectors, req.GetVectors())
	log.Printf("dimension of vector is %d",len(s.storedVectors[0].GetValues()))
	log.Printf("Stored %d vectors (replaced old data)\n", len(s.storedVectors))
	log.Printf("")
	return &pb.StoreResponse{
		Message: "Data stored successfully",
	}, nil
}

func distance(a, b []float32) float64 {
	// if len(a) != len(b) {
	// 	return -1
	// }

	var sum float64 = 0
	// log.Printf("Calculating distance between %v and %v\n", a, b)
	for i := range min(len(a),len(b)) {
		diff := float64(a[i] - b[i])
		sum += diff * diff
	}

	return math.Sqrt(sum)
}

func (s *server) NormalizeStored(ctx context.Context, req *pb.Empty) (*pb.VectorBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.storedVectors) == 0 {
		return &pb.VectorBatch{}, nil
	}

	var normalized []*pb.Vector

	for _, vec := range s.storedVectors {
		values := vec.GetValues()

		sum := float32(0)
		for _, v := range values {
			sum += v
		}

		newVals := make([]float32, len(values))
		if sum != 0 {
			for i, v := range values {
				newVals[i] = v / sum
			}
		}

		normalized = append(normalized, &pb.Vector{
			Values: newVals,
		})
	}

	log.Printf("Normalized %d stored vectors\n", len(normalized))

	return &pb.VectorBatch{
		Vectors: normalized,
	}, nil
}

func (s *server) KNN(ctx context.Context, req *pb.KNNRequest) (*pb.KNNResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := req.GetQuery()
	k := int(req.GetK())

	log.Printf("Received KNN request: query=%v, k=%d\n", query.GetValues(), k)

	if len(s.storedVectors) == 0 {
		return &pb.KNNResponse{}, nil
	}

	// Step 1: compute distances
	type pair struct {
		vec  *pb.Vector
		dist float64
	}

	var results []pair

	for _, v := range s.storedVectors {
		dist := distance(query.GetValues(), v.GetValues())
		results = append(results, pair{
			vec:  v,
			dist: dist,
		})
	}

	// Step 2: sort by distance
	sort.Slice(results, func(i, j int) bool {
		return results[i].dist < results[j].dist
	})

	// Step 3: limit k
	if k > len(results) {
		k = len(results)
	}

	// Step 4: collect top-k
	var neighbors []*pb.Vector
	var distances []float64
	for i := 0; i < k; i++ {
		neighbors = append(neighbors, results[i].vec)
		distances = append(distances, results[i].dist)
		log.Printf("Selected neighbor %d with distance %f\n", i, results[i].dist)
	}

	return &pb.KNNResponse{
		Neighbors: neighbors,
		Distances: distances,
	}, nil
}
// ------------------- main -------------------

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <port>")
	}

	port := os.Args[1]

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Server running on port %s...\n", port)

	grpcServer := grpc.NewServer()
	pb.RegisterGreeterServer(grpcServer, &server{})

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}