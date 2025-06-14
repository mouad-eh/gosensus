package raft

import (
	"crypto/sha256"
	"encoding/hex"

	_ "github.com/mattn/go-sqlite3"
	pb "github.com/mouad-eh/gosensus/rpc"
	"go.uber.org/zap"
)

type Server struct {
	pb.UnimplementedRaftClientServer
	pb.UnimplementedRaftNodeServer
	nodeID string
	// Map of node ID to RaftNodeClient
	peers map[string]pb.RaftNodeClient
	// State
	PersistentState
	VolatileState
	// Storage
	storage Storage
	// Logger
	logger *zap.SugaredLogger
}

type PersistentState struct {
	CurrentTerm  int32      `json:"current_term"`
	VotedFor     string     `json:"voted_for"`
	Log          []LogEntry `json:"log"`
	CommitLength int32      `json:"commit_length"`
}

type LogEntry struct {
	Message string `json:"message"`
	Term    int32  `json:"term"`
}

type VolatileState struct {
	CurrentRole   string
	CurrentLeader string
	VotesReceived map[string]struct{}
	SentLength    map[string]int32
	AckedLength   map[string]int32
}

// NewServer creates a new Raft server instance
func NewServer(nodeAddr string, leaderAddr string) *Server {
	nodeID := generateNodeID(nodeAddr)
	leaderID := generateNodeID(leaderAddr)

	currentRole := "follower"
	if leaderAddr == nodeAddr {
		currentRole = "leader"
	}

	return &Server{
		nodeID: nodeID,
		peers:  make(map[string]pb.RaftNodeClient),
		PersistentState: PersistentState{
			CurrentTerm:  0,
			VotedFor:     "",
			Log:          make([]LogEntry, 0),
			CommitLength: 0,
		},
		VolatileState: VolatileState{
			CurrentRole:   currentRole,
			CurrentLeader: leaderID,
			VotesReceived: make(map[string]struct{}),
			SentLength:    make(map[string]int32),
			AckedLength:   make(map[string]int32),
		},
		storage: NewJSONStorage(nodeID),
		logger:  NewSugaredZapLogger(nodeID),
	}
}

// generateNodeID creates a consistent node ID from IP address and port
func generateNodeID(addr string) string {
	// Create a hash of the address
	hash := sha256.Sum256([]byte(addr))
	// Take first 8 characters of the hex representation for a shorter ID
	return hex.EncodeToString(hash[:])[:8]
}
