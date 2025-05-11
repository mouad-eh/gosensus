package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	_ "time"

	pb "github.com/mouad-eh/gosensus/rpc"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/credentials/insecure"
)

type server struct {
	pb.UnimplementedRaftClientServer
	nodeID string
}

func (s *server) Broadcast(ctx context.Context, req *pb.BroadcastRequest) (*pb.BroadcastResponse, error) {
	infoLogger.Printf("Node %s Received broadcast request: %v", s.nodeID, req.GetMessage())
	// raft logic (TODO: make sure this thread is blocked until the leader delivers the message
	// to keep the communication between the client and the nodes synchronous)
	infoLogger.Printf("Node %s Acknowledged broadcast request: %v", s.nodeID, req.GetMessage())
	return &pb.BroadcastResponse{
		Success: true,
		NodeId:  s.nodeID,
	}, nil
}

var PORT = map[string]int{
	"1": 8001,
	"2": 8002,
	"3": 8003,
	"4": 8004,
	"5": 8005,
}

var infoLogger = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
var errorLogger = log.New(os.Stderr, "", log.Ltime|log.Lshortfile)

func main() {
	// Check if this is a child process
	if len(os.Args) > 1 && os.Args[1] == "child" {
		// This is a child process
		nodeID := os.Args[2]

		infoLogger.SetPrefix(fmt.Sprintf("[INFO] Node %s: ", nodeID))
		errorLogger.SetPrefix(fmt.Sprintf("[ERROR] Node %s: ", nodeID))

		infoLogger.Printf("Starting ...")

		// Start the Raft node

		port := PORT[nodeID]

		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			errorLogger.Fatal("Error starting server:", err)
		}
		defer listener.Close()

		infoLogger.Printf("Listening on port %d\n", port)

		s := grpc.NewServer()
		pb.RegisterRaftClientServer(s, &server{nodeID: nodeID})
		// Serve will start the server and block until the server is stopped
		if err := s.Serve(listener); err != nil {
			errorLogger.Fatalf("failed to serve: %v", err)
		}
		return
	}

	infoLogger.SetPrefix("[INFO] Initiator: ")
	errorLogger.SetPrefix("[ERROR] Initiator: ")

	// Parent process: spawn child processes
	numNodes := 5 // Number of Raft nodes

	var nodes []*exec.Cmd
	for i := 1; i <= numNodes; i++ {
		nodeID := strconv.Itoa(i)

		// Get the path to the current executable
		executable, err := os.Executable()
		if err != nil {
			errorLogger.Fatalf("Failed to get executable path: %v", err)
		}

		// Create the command to run a child process
		cmd := exec.Command(executable, "child", nodeID)
		nodes = append(nodes, cmd)

		// Set up pipes for stdout and stderr
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// Start the process
		err = cmd.Start()
		if err != nil {
			errorLogger.Fatalf("Failed to start node %s: %v", nodeID, err)
		}

		infoLogger.Printf("Started node %s with PID %d\n", nodeID, cmd.Process.Pid)
	}

	for _, cmd := range nodes {
		err := cmd.Wait()
		if err != nil {
			errorLogger.Fatalf("Node %s exited with error: %v", cmd.Args[2], err)
		}
	}

	// Wait for all processes to finish if desired

	infoLogger.Println("All nodes finished execution")
}
