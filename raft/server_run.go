package raft

import (
	"fmt"
	"net"
	"os"

	pb "github.com/mouad-eh/gosensus/rpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *Server) Run(clientPort int, nodePort int, peerAddrs []string) error {
	// Initialize storage
	if err := s.storage.Init(s.nodeID); err != nil {
		return fmt.Errorf("failed to initialize storage: %v", err)
	}

	// Load persistent state from storage
	currentTerm, votedFor, commitLength, log, err := s.storage.LoadState()
	if err != nil {
		return fmt.Errorf("failed to load state from storage: %v", err)
	}
	s.CurrentTerm = currentTerm
	s.VotedFor = votedFor
	s.CommitLength = commitLength
	s.Log = log

	// Connect to all peers
	if err := s.connectToPeers(peerAddrs); err != nil {
		return fmt.Errorf("failed to connect to peers: %v", err)
	}

	// Start servers
	if err := s.startServers(clientPort, nodePort); err != nil {
		return fmt.Errorf("failed to start servers: %v", err)
	}

	return nil
}

// connectToPeers establishes connections to all peer nodes
// TODO: rename this function
func (s *Server) connectToPeers(peerAddrs []string) error {
	for _, peerAddr := range peerAddrs {
		if peerAddr == "" {
			continue
		}
		peerID := generateNodeID(peerAddr)
		conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to peer %s at %s: %v", peerID, peerAddr, err)
		}
		s.peers[peerID] = pb.NewRaftNodeClient(conn)
		s.SentLength[peerID] = 0
		s.AckedLength[peerID] = 0
		s.logger.Infow("Connected to peer", "peer_id", peerID, "address", peerAddr)
	}
	return nil
}

// startServers starts both the client and node Servers
func (s *Server) startServers(clientPort, nodePort int) error {
	// Start client Server
	clientListener, err := net.Listen("tcp", fmt.Sprintf(":%d", clientPort))
	if err != nil {
		return fmt.Errorf("error starting client Server: %v", err)
	}
	defer clientListener.Close()

	s.logger.Infow("Client Server listening", "port", clientPort)

	clientServer := grpc.NewServer()
	pb.RegisterRaftClientServer(clientServer, s)

	// Start node Server
	nodeListener, err := net.Listen("tcp", fmt.Sprintf(":%d", nodePort))
	if err != nil {
		return fmt.Errorf("error starting node Server: %v", err)
	}
	defer nodeListener.Close()

	s.logger.Infow("Node Server listening", "port", nodePort)

	nodeServer := grpc.NewServer()
	pb.RegisterRaftNodeServer(nodeServer, s)

	// Start both Servers in separate goroutines
	go func() {
		if err := clientServer.Serve(clientListener); err != nil {
			s.logger.Errorw("Failed to serve client Server", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		if err := nodeServer.Serve(nodeListener); err != nil {
			s.logger.Errorw("Failed to serve node Server", "error", err)
			os.Exit(1)
		}
	}()

	// Wait indefinitely
	select {}
}
