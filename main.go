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

func main() {
	// Check if this is a child process
	if len(os.Args) > 1 && os.Args[1] == "child" {
		// This is a child process
		nodeID := os.Args[2]
		fmt.Printf("I am node %s in the Raft cluster\n", nodeID)

		// Start the Raft node

		port := PORT[nodeID]

		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			fmt.Println("Error starting server:", err)
			return
		}
		defer listener.Close()

		fmt.Printf("Node %s is listening on port %d\n", nodeID, port)

		if nodeID == "1" {
			// Node 1 sends a message to Node 2
			var conn net.Conn
			var err error
			for {
				conn, err = net.Dial("tcp", "localhost:"+strconv.Itoa(PORT["2"]))
				if err == nil {
					break
				}
				fmt.Println("Waiting for Node 2 to start listening...")
				time.Sleep(1 * time.Second) // Wait before retrying
			}
			defer conn.Close()
			fmt.Println("Node 1 is sending a message to Node 2")
			conn.Write([]byte("Hello, Node 2!"))
		}
		if nodeID == "2" {
			// Node 2 receives a message from Node 1
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("Error accepting connection:", err)
				return
			}
			defer conn.Close()

			message := make([]byte, 1024)
			numBytes, err := conn.Read(message)
			if err != nil {
				fmt.Println("Error reading:", err)
				return
			}
			fmt.Printf("Received %d bytes from node 1: %s\n", numBytes, string(message))
		}
		return
	}

	// Parent process: spawn child processes
	numNodes := 5 // Number of Raft nodes

	for i := 1; i <= numNodes; i++ {
		nodeID := strconv.Itoa(i)

		// Get the path to the current executable
		executable, err := os.Executable()
		if err != nil {
			log.Fatalf("Failed to get executable path: %v", err)
		}

		// Create the command to run a child process
		cmd := exec.Command(executable, "child", nodeID)

		// Set up pipes for stdout and stderr
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// Start the process
		err = cmd.Start()
		if err != nil {
			log.Fatalf("Failed to start node %s: %v", nodeID, err)
		}

		fmt.Printf("Started node %s with PID %d\n", nodeID, cmd.Process.Pid)
	}

	// Wait for all processes to finish if desired

	fmt.Println("All nodes started")
}
