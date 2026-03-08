package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
	"strings"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "github.com/viratgarg/grpc-demo/proto"
	"sort"
)

func ConnectToServer(address string) (*grpc.ClientConn, error) {
	return grpc.Dial(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func TakeInputAndSend(fileName string, clients []pb.GreeterClient) {

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Error opening file:", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal("Error reading CSV:", err)
	}

	// Convert to vectors
	var allVectors []*pb.Vector

	for _, record := range records {
		var values []float32

		for _, value := range record {
			value = strings.TrimSpace(value)
			num, err := strconv.ParseFloat(value, 32)
			if err != nil {
				log.Printf("Skipping non-numeric value: %q", value)
				continue
			}
			values = append(values, float32(num))
		}

		allVectors = append(allVectors, &pb.Vector{
			Values: values,
		})
	}

	N := len(allVectors)
	S := len(clients)

	baseSize := N / S
	remainder := N % S
	start := 0

	// ----------- DISTRIBUTE BLOCKS -----------
	// log.Printf("vector has dimension %d",len(allVectors[0]))
	for i := 0; i < S; i++ {

		chunkSize := baseSize
		if i < remainder {
			chunkSize++
		}

		end := start + chunkSize
		if end > N {
			end = N
		}

		chunk := allVectors[start:end]
		start = end

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		_, err := clients[i].StoreBatch(ctx, &pb.VectorBatch{
			Vectors: chunk,
		})

		cancel()

		if err != nil {
			log.Println("Store RPC error:", err)
			continue
		}

		log.Printf("Sent %d vectors to server %d\n", len(chunk), i)
	}

}

func formatVector(values []float32) string {
	var result string
	result += "("
	for i, v := range values {
		// log.Printf("%d %.2f",i,v)
		result += fmt.Sprintf("%.1f", v)
		if i != len(values)-1 {
			result += " , "
		}
	}

	result += ")"
	return result
}
type pair struct {
		vec  *pb.Vector
		dist float64
}

func sendKNNRequest(clients []pb.GreeterClient, queryVector []float32, k int) {
	var allResults []pair
	for i, c := range clients {

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		resp, err := c.KNN(ctx, &pb.KNNRequest{
			Query: &pb.Vector{
				Values: queryVector,
			},
			K: int32(k),
		})

		cancel()

		if err != nil {
			log.Printf("KNN RPC error from server %d: %v\n", i, err)
			continue
		}

		for j := range resp.Neighbors {
			allResults = append(allResults, pair{
				vec:  resp.Neighbors[j],
				dist: resp.Distances[j],
			})
		}
	}
	if len(allResults) == 0 {
		fmt.Println("No vectors found.")
		return
	}
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].dist < allResults[j].dist
	})
	if k > len(allResults) {
		k = len(allResults)
	}
	fmt.Printf("Global %d - Nearest Neighbours of %s:\n",
		k,
		formatVector(queryVector),
	)

	for i := 0; i < k; i++ {
		fmt.Printf("%d %s - distance : %.2f\n",
			i+1,
			formatVector(allResults[i].vec.Values),
			allResults[i].dist,
		)
	}
}


func main() {

	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <port1> <port2> ...")
	}

	var clients []pb.GreeterClient
	var conns []*grpc.ClientConn

	for _, arg := range os.Args[1:] {
		addr := "localhost:" + arg

		conn, err := ConnectToServer(addr)
		if err != nil {
			log.Fatal(err)
		}

		conns = append(conns, conn)
		clients = append(clients, pb.NewGreeterClient(conn))
	}

	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	for {
		fmt.Println("\n1 -> Load CSV & Distribute")
		fmt.Println("2-> KNN")
		fmt.Println("3 -> Exit")

		var choice int
		fmt.Scan(&choice)

		if choice == 3 {
			break
		}

		if choice == 1 {
			fmt.Println("Enter CSV filename (inside data folder):")
			var filename string
			fmt.Scan(&filename)

			TakeInputAndSend("data/"+filename, clients)
		}

		if choice == 2 {
			fmt.Println("Enter query vector (comma separated):")
			var input string
			fmt.Scan(&input)

			valuesStr := csv.NewReader(strings.NewReader(input))
			values, err := valuesStr.Read()
			if err != nil {
				log.Println("Invalid input:", err)
				continue
			}

			var QueryVector []float32
			for _, v := range values {
				num, err := strconv.ParseFloat(v, 32)
				if err != nil {
					log.Println("Invalid number:", err)
					continue
				}
				QueryVector = append(QueryVector, float32(num))
			}

			fmt.Println("Enter k:")
			var k int
			fmt.Scan(&k)

			sendKNNRequest(clients, QueryVector, k)

		}
	}
}