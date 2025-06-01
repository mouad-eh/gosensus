package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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

type server struct {
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
	// Database connection
	db *sql.DB
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

var infoLogger = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
var errorLogger = log.New(os.Stderr, "", log.Ltime|log.Lshortfile)

func (s *server) initDB() error {
	// Create data directory if it doesn't exist
	stateDir := filepath.Join("state", s.nodeID)
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}

	// Open SQLite database
	dbPath := filepath.Join(stateDir, "raft.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	s.db = db

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS state (
			key TEXT PRIMARY KEY,
			value TEXT
		);
		CREATE TABLE IF NOT EXISTS log_entries (
			entry_index INTEGER PRIMARY KEY,
			term INTEGER,
			message TEXT
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create tables: %v", err)
	}

	return nil
}

func (s *server) saveToDisk() error {
	// Save current term
	_, err := s.db.Exec("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
		"current_term", s.currentTerm)
	if err != nil {
		return fmt.Errorf("failed to save current term: %v", err)
	}

	// Save voted for
	_, err = s.db.Exec("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
		"voted_for", s.votedFor)
	if err != nil {
		return fmt.Errorf("failed to save voted for: %v", err)
	}

	// Save commit length
	_, err = s.db.Exec("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
		"commit_length", s.commitLength)
	if err != nil {
		return fmt.Errorf("failed to save commit length: %v", err)
	}

	// Save log entries
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Clear existing log entries
	_, err = tx.Exec("DELETE FROM log_entries")
	if err != nil {
		return fmt.Errorf("failed to clear log entries: %v", err)
	}

	// Insert new log entries
	stmt, err := tx.Prepare("INSERT INTO log_entries (entry_index, term, message) VALUES (?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare log insert statement: %v", err)
	}
	defer stmt.Close()

	for i, entry := range s.log {
		_, err = stmt.Exec(i, entry.Term, entry.Message)
		if err != nil {
			return fmt.Errorf("failed to insert log entry: %v", err)
		}
	}

	return tx.Commit()
}

func (s *server) loadFromDisk() error {
	// Load current term
	var currentTermStr string
	err := s.db.QueryRow("SELECT value FROM state WHERE key = ?", "current_term").Scan(&currentTermStr)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to load current term: %v", err)
	}
	if currentTermStr != "" {
		term, err := strconv.ParseInt(currentTermStr, 10, 32)
		if err != nil {
			return fmt.Errorf("failed to parse current term: %v", err)
		}
		s.currentTerm = int32(term)
	}

	// Load voted for
	err = s.db.QueryRow("SELECT value FROM state WHERE key = ?", "voted_for").Scan(&s.votedFor)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to load voted for: %v", err)
	}

	// Load commit length
	var commitLengthStr string
	err = s.db.QueryRow("SELECT value FROM state WHERE key = ?", "commit_length").Scan(&commitLengthStr)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to load commit length: %v", err)
	}
	if commitLengthStr != "" {
		length, err := strconv.ParseInt(commitLengthStr, 10, 32)
		if err != nil {
			return fmt.Errorf("failed to parse commit length: %v", err)
		}
		s.commitLength = int32(length)
	}

	// Load log entries
	rows, err := s.db.Query("SELECT term, message FROM log_entries ORDER BY entry_index")
	if err != nil {
		return fmt.Errorf("failed to query log entries: %v", err)
	}
	defer rows.Close()

	s.log = []pb.LogEntry{}
	for rows.Next() {
		var term int32
		var message string
		if err := rows.Scan(&term, &message); err != nil {
			return fmt.Errorf("failed to scan log entry: %v", err)
		}
		s.log = append(s.log, pb.LogEntry{
			Term:    term,
			Message: message,
		})
	}

	return rows.Err()
}

// Update the setter methods to save state after modification
func (s *server) setCurrentTerm(term int32) {
	s.currentTerm = term
	if err := s.saveToDisk(); err != nil {
		errorLogger.Printf("Failed to save state after setting term: %v", err)
	}
}

func (s *server) setVotedFor(votedFor string) {
	s.votedFor = votedFor
	if err := s.saveToDisk(); err != nil {
		errorLogger.Printf("Failed to save state after setting votedFor: %v", err)
	}
}

func (s *server) appendLog(entry pb.LogEntry) {
	s.log = append(s.log, entry)
	if err := s.saveToDisk(); err != nil {
		errorLogger.Printf("Failed to save state after appending log: %v", err)
	}
}

func (s *server) setCommitLength(length int32) {
	s.commitLength = length
	if err := s.saveToDisk(); err != nil {
		errorLogger.Printf("Failed to save state after setting commit length: %v", err)
	}
}

// generateNodeID creates a consistent node ID from IP address and port
func generateNodeID(addr string) string {
	// Create a hash of the address
	hash := sha256.Sum256([]byte(addr))
	// Take first 8 characters of the hex representation for a shorter ID
	return hex.EncodeToString(hash[:])[:8]
}

func main() {
	// Define command line flags
	ipAddr := flag.String("ip", "localhost", "Node IP address")
	clientPort := flag.Int("client-port", 0, "Client server port")
	nodePort := flag.Int("node-port", 0, "Node server port")
	peersStr := flag.String("peers", "", "Comma-separated list of peer addresses (e.g., localhost:9002,localhost:9003)")
	leaderAddr := flag.String("leader", "", "Address of the designated leader node (e.g., localhost:9001)")
	flag.Parse()

	// Validate required flags
	if *clientPort == 0 || *nodePort == 0 {
		fmt.Println("Error: --client-port and --node-port are required")
		flag.Usage()
		os.Exit(1)
	}

	// Generate node address and ID
	nodeAddr := fmt.Sprintf("%s:%d", *ipAddr, *nodePort)
	nodeID := generateNodeID(nodeAddr)

	// Generate leader ID if leader address is provided
	var leaderID string
	if *leaderAddr != "" {
		leaderID = generateNodeID(*leaderAddr)
	}

	// Set initial role
	currentRole := "follower"
	if *leaderAddr == nodeAddr {
		currentRole = "leader"
	}

	// Parse peer addresses
	peerAddrs := strings.Split(*peersStr, ",")
	for i := range peerAddrs {
		peerAddrs[i] = strings.TrimSpace(peerAddrs[i])
	}

	infoLogger.SetPrefix(fmt.Sprintf("[INFO] Node %s: ", nodeID))
	errorLogger.SetPrefix(fmt.Sprintf("[ERROR] Node %s: ", nodeID))

	infoLogger.Printf("Starting node %s (%s) with client port %d and node port %d", nodeID, nodeAddr, *clientPort, *nodePort)

	// Create server instance
	s := &server{
		nodeID:        nodeID,
		peers:         make(map[string]pb.RaftNodeClient),
		currentRole:   currentRole,
		currentLeader: leaderID,
		votesReceived: make(map[string]struct{}),
		sentLength:    make(map[string]int32),
		ackedLength:   make(map[string]int32),
	}

	// Initialize database
	if err := s.initDB(); err != nil {
		errorLogger.Fatalf("Failed to initialize database: %v", err)
	}
	defer s.db.Close()

	// Load persistent state from disk
	if err := s.loadFromDisk(); err != nil {
		errorLogger.Printf("Failed to load state from disk: %v", err)
	}

	// Connect to all peers
	for _, peerAddr := range peerAddrs {
		if peerAddr == "" {
			continue
		}
		peerID := generateNodeID(peerAddr)
		conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			errorLogger.Fatalf("Failed to connect to peer %s at %s: %v", peerID, peerAddr, err)
		}
		s.peers[peerID] = pb.NewRaftNodeClient(conn)
		infoLogger.Printf("Connected to peer %s at %s", peerID, peerAddr)
	}

	// Start client server
	clientListener, err := net.Listen("tcp", fmt.Sprintf(":%d", *clientPort))
	if err != nil {
		errorLogger.Fatal("Error starting client server:", err)
	}
	defer clientListener.Close()

	infoLogger.Printf("Client server listening on port %d", *clientPort)

	clientServer := grpc.NewServer()
	pb.RegisterRaftClientServer(clientServer, s)

	// Start node server
	nodeListener, err := net.Listen("tcp", fmt.Sprintf(":%d", *nodePort))
	if err != nil {
		errorLogger.Fatal("Error starting node server:", err)
	}
	defer nodeListener.Close()

	infoLogger.Printf("Node server listening on port %d", *nodePort)

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
}
