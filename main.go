package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	raft "github.com/mouad-eh/gosensus/raft"
)

func main() {
	// Define command line flags
	ipAddr := flag.String("ip", "localhost", "Node IP address")
	clientPort := flag.Int("client-port", 0, "Client server port")
	nodePort := flag.Int("node-port", 0, "Node server port")
	peersStr := flag.String("peers", "", "Comma-separated list of peer addresses (e.g., localhost:9002,localhost:9003)")
	leaderAddr := flag.String("leader", "", "Address of the designated leader node (e.g., localhost:9001)")
	flag.Parse()

	// Validate required flags
	if *clientPort == 0 || *nodePort == 0 || *peersStr == "" || *leaderAddr == "" {
		fmt.Println("Error: --client-port, --node-port, --peers, and --leader are required")
		flag.Usage()
		os.Exit(1)
	}

	// Generate node address and ID
	nodeAddr := fmt.Sprintf("%s:%d", *ipAddr, *nodePort)

	// Parse peer addresses
	peerAddrs := strings.Split(*peersStr, ",")
	for i := range peerAddrs {
		peerAddrs[i] = strings.TrimSpace(peerAddrs[i])
	}

	// Start the server
	server := raft.NewServer(nodeAddr, *leaderAddr)
	err := server.Run(*clientPort, *nodePort, peerAddrs)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
}
