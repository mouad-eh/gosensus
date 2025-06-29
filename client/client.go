package main

import (
	"context"
	"flag"
	"log"
	"time"

	pb "github.com/mouad-eh/gosensus/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	nodeAddr = flag.String("node", "localhost:8001", "The address of the node to connect to")
	message  = flag.String("message", "Hello from client", "The message to broadcast")
)

func main() {
	flag.Parse()

	// Set up connection to the node
	conn, err := grpc.NewClient(*nodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	// Create client
	c := pb.NewRaftClientClient(conn)

	// Set timeout context
	// Timout must be greater than the heartbeat timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Send broadcast request
	resp, err := c.Broadcast(ctx, &pb.BroadcastRequest{Message: *message})
	if err != nil {
		log.Fatalf("could not broadcast: %v", err)
	}

	log.Printf("Broadcast response from node %s: %v", resp.GetNodeId(), resp.GetSuccess())
}
