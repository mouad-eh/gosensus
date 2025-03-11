package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"

	pb "github.com/mouad-eh/gosensus/rpcmessage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type server struct {
	pb.UnimplementedGreeterServer
}

func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	infoLogger.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
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

		if nodeID == "1" {
			// Node 1 sends a message to Node 2
			conn, err := grpc.NewClient("localhost:"+strconv.Itoa(PORT["2"]), grpc.WithTransportCredentials(insecure.NewCredentials()))
			conn.Connect()
			if err != nil {
				errorLogger.Fatalf("did not connect: %v", err)
			}
			defer conn.Close()
			c := pb.NewGreeterClient(conn)

			// Contact the server and print out its response
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			var r *pb.HelloReply
			for {
				r, err = c.SayHello(ctx, &pb.HelloRequest{Name: "node1"})
				if err == nil {
					break
				}
				infoLogger.Println("Waiting for Node 2 to start...")
				time.Sleep(1 * time.Second) // Wait before retrying
			}
			infoLogger.Printf("Receiving: %s", r.GetMessage())
		}
		if nodeID == "2" {
			// Node 2 receives a message from Node 1
			s := grpc.NewServer()
			pb.RegisterGreeterServer(s, &server{})
			// Serve will start the server and block until the server is stopped
			if err := s.Serve(listener); err != nil {
				errorLogger.Fatalf("failed to serve: %v", err)
			}
		}
		infoLogger.Printf("%s is about to return...\n", nodeID)
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
