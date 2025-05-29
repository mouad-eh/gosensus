package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"

	pb "github.com/mouad-eh/gosensus/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	pb.UnimplementedRaftClientServer
	pb.UnimplementedRaftNodeServer
	nodeID string
	// Map of node ID to RaftNodeClient
	peers map[string]pb.RaftNodeClient
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

// RaftNodeServer implementation
func (s *server) RequestVote(ctx context.Context, req *pb.VoteRequest) (*emptypb.Empty, error) {
	infoLogger.Printf("Node %s received vote request from %s for term %d", s.nodeID, req.CandidateId, req.Term)
	return &emptypb.Empty{}, nil
}

func (s *server) HandleVoteResponse(ctx context.Context, resp *pb.VoteResponse) (*emptypb.Empty, error) {
	infoLogger.Printf("Node %s received vote response from %s: granted=%v for term %d",
		s.nodeID, resp.VoterId, resp.Granted, resp.Term)
	return &emptypb.Empty{}, nil
}

func (s *server) RequestLog(ctx context.Context, req *pb.LogRequest) (*emptypb.Empty, error) {
	infoLogger.Printf("Node %s received log request from leader %s for term %d",
		s.nodeID, req.LeaderId, req.Term)
	return &emptypb.Empty{}, nil
}

func (s *server) HandleLogResponse(ctx context.Context, resp *pb.LogResponse) (*emptypb.Empty, error) {
	infoLogger.Printf("Node %s received log response from follower %s: success=%v for term %d",
		s.nodeID, resp.FollowerId, resp.Success, resp.Term)
	return &emptypb.Empty{}, nil
}

// Ports for client-server communication
var CLIENT_PORT = map[string]int{
	"1": 8001,
	"2": 8002,
	"3": 8003,
	"4": 8004,
	"5": 8005,
}

// Ports for inter-node communication
var NODE_PORT = map[string]int{
	"1": 9001,
	"2": 9002,
	"3": 9003,
	"4": 9004,
	"5": 9005,
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

		// Create server instance
		s := &server{
			nodeID: nodeID,
			peers:  make(map[string]pb.RaftNodeClient),
		}

		// Connect to all other nodes
		for id, port := range NODE_PORT {
			if id != nodeID {
				conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					errorLogger.Fatalf("Failed to connect to node %s: %v", id, err)
				}
				s.peers[id] = pb.NewRaftNodeClient(conn)
				infoLogger.Printf("Connected to node %s", id)
			}
		}

		// Start client server
		clientPort := CLIENT_PORT[nodeID]
		clientListener, err := net.Listen("tcp", fmt.Sprintf(":%d", clientPort))
		if err != nil {
			errorLogger.Fatal("Error starting client server:", err)
		}
		defer clientListener.Close()

		infoLogger.Printf("Client server listening on port %d\n", clientPort)

		clientServer := grpc.NewServer()
		pb.RegisterRaftClientServer(clientServer, s)

		// Start node server
		nodePort := NODE_PORT[nodeID]
		nodeListener, err := net.Listen("tcp", fmt.Sprintf(":%d", nodePort))
		if err != nil {
			errorLogger.Fatal("Error starting node server:", err)
		}
		defer nodeListener.Close()

		infoLogger.Printf("Node server listening on port %d\n", nodePort)

		nodeServer := grpc.NewServer()
		pb.RegisterRaftNodeServer(nodeServer, s)

		// Start both servers in separate goroutines
		go func() {
			if err := clientServer.Serve(clientListener); err != nil {
				errorLogger.Fatalf("failed to serve client server: %v", err)
			}
		}()

		go func() {
			if err := nodeServer.Serve(nodeListener); err != nil {
				errorLogger.Fatalf("failed to serve node server: %v", err)
			}
		}()

		// Wait indefinitely
		select {}

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

	infoLogger.Println("All nodes finished execution")
}
