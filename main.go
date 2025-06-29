package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	raft "github.com/mouad-eh/gosensus/raft"
)

func main() {
	// Define command line flags
	ipAddr := flag.String("ip", "localhost", "Node IP address")
	clientPort := flag.Int("client-port", 0, "Client server port")
	nodePort := flag.Int("node-port", 0, "Node server port")
	peersStr := flag.String("peers", "", "Comma-separated list of peer addresses (e.g., localhost:9002,localhost:9003)")
	flag.Parse()

	// Validate required flags
	if *clientPort == 0 || *nodePort == 0 || *peersStr == "" {
		fmt.Println("Error: --client-port, --node-port, and --peers are required")
		flag.Usage()
		os.Exit(1)
	}

	nodeIP := net.ParseIP(*ipAddr)
	if nodeIP == nil {
		fmt.Printf("Error: invalid IP address: %s\n", *ipAddr)
		os.Exit(1)
	}

	if *nodePort < 0 || *nodePort > 65535 {
		fmt.Printf("Error: node port must be between 0 and 65535\n")
		os.Exit(1)
	}

	if *clientPort < 0 || *clientPort > 65535 {
		fmt.Printf("Error: client port must be between 0 and 65535\n")
		os.Exit(1)
	}

	peers := make([]*net.TCPAddr, 0)
	for _, addr := range strings.Split(*peersStr, ",") {
		peerIP, err := net.ResolveTCPAddr("tcp", addr)
		if err != nil {
			fmt.Printf("Error resolving peer address: %v\n", err)
			os.Exit(1)
		}
		peers = append(peers, peerIP)
	}

	// Create gRPC server configuration
	config := &raft.GRPCServerConfig{
		IP:         nodeIP,
		NodePort:   *nodePort,
		ClientPort: *clientPort,
		Peers:      peers,
	}

	// Create and run the gRPC server
	server := raft.NewgRPCServer(config)
	if err := server.Run(); err != nil {
		fmt.Printf("Error running server: %v\n", err)
		os.Exit(1)
	}
}
