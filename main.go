package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"
)

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
			errorLogger.Println("Error starting server:", err)
			return
		}
		defer listener.Close()

		infoLogger.Printf("Listening on port %d\n", port)

		if nodeID == "1" {
			// Node 1 sends a message to Node 2
			var conn net.Conn
			var err error
			for {
				conn, err = net.Dial("tcp", "localhost:"+strconv.Itoa(PORT["2"]))
				if err == nil {
					break
				}
				infoLogger.Println("Waiting for Node 2 to start listening...")
				time.Sleep(1 * time.Second) // Wait before retrying
			}
			defer conn.Close()

			infoLogger.Println("Sending a message to Node 2 ...")
			conn.Write([]byte("Hello, Node 2!"))
		}
		if nodeID == "2" {
			// Node 2 receives a message from Node 1
			conn, err := listener.Accept()
			if err != nil {
				errorLogger.Println("Error accepting connection:", err)
				return
			}
			defer conn.Close()

			message := make([]byte, 1024)
			numBytes, err := conn.Read(message)
			if err != nil {
				errorLogger.Println("Error reading:", err)
				return
			}
			infoLogger.Printf("Received %d bytes from node 1: %s\n", numBytes, string(message))
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
