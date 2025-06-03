package raft

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"

	_ "github.com/mattn/go-sqlite3"
	pb "github.com/mouad-eh/gosensus/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// DiskState represents the persistent state stored on disk
type DiskState struct {
	CurrentTerm  int32         `json:"current_term"`
	VotedFor     string        `json:"voted_for"`
	Log          []pb.LogEntry `json:"log"`
	CommitLength int32         `json:"commit_length"`
}

type Server struct {
	pb.UnimplementedRaftClientServer
	pb.UnimplementedRaftNodeServer
	nodeID string
	// Map of node ID to RaftNodeClient
	peers map[string]pb.RaftNodeClient
	// DISK
	currentTerm  int32
	votedFor     string
	log          []pb.LogEntry
	commitLength int32
	// RAM
	currentRole   string
	currentLeader string
	votesReceived map[string]struct{}
	sentLength    map[string]int32
	ackedLength   map[string]int32
	// Storage
	storage Storage
	// Logger
	logger *Logger
}

// Update the setter methods to use the storage interface
func (s *Server) setCurrentTerm(term int32) {
	s.currentTerm = term
	if err := s.storage.SaveState(s.currentTerm, s.votedFor, s.commitLength, s.log); err != nil {
		s.logger.Error("Failed to save state after setting term: %v", err)
	}
}

func (s *Server) setVotedFor(votedFor string) {
	s.votedFor = votedFor
	if err := s.storage.SaveState(s.currentTerm, s.votedFor, s.commitLength, s.log); err != nil {
		s.logger.Error("Failed to save state after setting votedFor: %v", err)
	}
}

func (s *Server) appendLog(entry pb.LogEntry) {
	s.log = append(s.log, entry)
	if err := s.storage.SaveState(s.currentTerm, s.votedFor, s.commitLength, s.log); err != nil {
		s.logger.Error("Failed to save state after appending log: %v", err)
	}
}

func (s *Server) setCommitLength(length int32) {
	s.commitLength = length
	if err := s.storage.SaveState(s.currentTerm, s.votedFor, s.commitLength, s.log); err != nil {
		s.logger.Error("Failed to save state after setting commit length: %v", err)
	}
}

// NewServer creates a new Raft server instance
func NewServer(nodeAddr string, leaderAddr string) *Server {
	nodeID := generateNodeID(nodeAddr)

	// Generate leader ID if leader address is provided
	var leaderID string
	if leaderAddr != "" {
		leaderID = generateNodeID(leaderAddr)
	}

	// Set initial role
	currentRole := "follower"
	if leaderAddr == nodeAddr {
		currentRole = "leader"
	}

	return &Server{
		nodeID:        nodeID,
		peers:         make(map[string]pb.RaftNodeClient),
		currentRole:   currentRole,
		currentLeader: leaderID,
		votesReceived: make(map[string]struct{}),
		sentLength:    make(map[string]int32),
		ackedLength:   make(map[string]int32),
		storage:       NewSQLiteStorage(),
		logger:        NewLogger(nodeID),
	}
}

// generateNodeID creates a consistent node ID from IP address and port
func generateNodeID(addr string) string {
	// Create a hash of the address
	hash := sha256.Sum256([]byte(addr))
	// Take first 8 characters of the hex representation for a shorter ID
	return hex.EncodeToString(hash[:])[:8]
}

func (s *Server) Broadcast(ctx context.Context, req *pb.BroadcastRequest) (*pb.BroadcastResponse, error) {
	s.logger.Info("Received broadcast request: %v", req.GetMessage())
	// raft logic (TODO: make sure this thread is blocked until the leader delivers the message
	// to keep the communication between the client and the nodes synchronous)
	s.logger.Info("Acknowledged broadcast request: %v", req.GetMessage())
	return &pb.BroadcastResponse{
		Success: true,
		NodeId:  s.nodeID,
	}, nil
}

// RaftNodeServer implementation
func (s *Server) RequestVote(ctx context.Context, req *pb.VoteRequest) (*emptypb.Empty, error) {
	s.logger.Info("Received vote request from %s for term %d", req.CandidateId, req.Term)
	return &emptypb.Empty{}, nil
}

func (s *Server) HandleVoteResponse(ctx context.Context, resp *pb.VoteResponse) (*emptypb.Empty, error) {
	s.logger.Info("Received vote response from %s: granted=%v for term %d",
		resp.VoterId, resp.Granted, resp.Term)
	return &emptypb.Empty{}, nil
}

func (s *Server) RequestLog(ctx context.Context, req *pb.LogRequest) (*emptypb.Empty, error) {
	s.logger.Info("Received log request from leader %s for term %d",
		req.LeaderId, req.Term)
	return &emptypb.Empty{}, nil
}

func (s *Server) HandleLogResponse(ctx context.Context, resp *pb.LogResponse) (*emptypb.Empty, error) {
	s.logger.Info("Received log response from follower %s: success=%v for term %d",
		resp.FollowerId, resp.Success, resp.Term)
	return &emptypb.Empty{}, nil
}

func (s *Server) Run(clientPort int, nodePort int, peerAddrs []string) error {
	// Initialize storage
	if err := s.storage.Init(s.nodeID); err != nil {
		return fmt.Errorf("failed to initialize storage: %v", err)
	}
	defer s.storage.Close()

	// Load persistent state from storage
	currentTerm, votedFor, commitLength, log, err := s.storage.LoadState()
	if err != nil {
		s.logger.Error("Failed to load state from storage: %v", err)
	} else {
		s.currentTerm = currentTerm
		s.votedFor = votedFor
		s.commitLength = commitLength
		s.log = log
	}

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
		s.logger.Info("Connected to peer %s at %s", peerID, peerAddr)
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

	s.logger.Info("Client Server listening on port %d", clientPort)

	clientServer := grpc.NewServer()
	pb.RegisterRaftClientServer(clientServer, s)

	// Start node Server
	nodeListener, err := net.Listen("tcp", fmt.Sprintf(":%d", nodePort))
	if err != nil {
		return fmt.Errorf("error starting node Server: %v", err)
	}
	defer nodeListener.Close()

	s.logger.Info("Node Server listening on port %d", nodePort)

	nodeServer := grpc.NewServer()
	pb.RegisterRaftNodeServer(nodeServer, s)

	// Start both Servers in separate goroutines
	go func() {
		if err := clientServer.Serve(clientListener); err != nil {
			s.logger.Error("failed to serve client Server: %v", err)
			os.Exit(1)
		}
	}()

	go func() {
		if err := nodeServer.Serve(nodeListener); err != nil {
			s.logger.Error("failed to serve node Server: %v", err)
			os.Exit(1)
		}
	}()

	// Wait indefinitely
	select {}
}
