package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

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

func (s *server) initDB() error {
	// Create data directory if it doesn't exist
	dataDir := filepath.Join("data", s.nodeID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}

	// Open SQLite database
	dbPath := filepath.Join(dataDir, "raft.db")
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
			nodeID:        nodeID,
			peers:         make(map[string]pb.RaftNodeClient),
			currentRole:   "follower",
			currentLeader: "",
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
