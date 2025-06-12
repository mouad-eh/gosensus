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
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
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

func (s *Server) setCurrentTerm(term int32) error {
	s.CurrentTerm = term
	if err := s.storage.SaveCurrentTerm(term); err != nil {
		return fmt.Errorf("failed to save state after setting term: %w", err)
	}
	return nil
}

func (s *Server) setVotedFor(votedFor string) error {
	s.VotedFor = votedFor
	if err := s.storage.SaveVotedFor(votedFor); err != nil {
		return fmt.Errorf("failed to save state after setting votedFor: %w", err)
	}
	return nil
}

func (s *Server) appendLog(entry LogEntry) error {
	s.Log = append(s.Log, entry)
	if err := s.storage.AppendLog(entry); err != nil {
		return fmt.Errorf("failed to save state after appending log: %w", err)
	}
	return nil
}

func (s *Server) trimLog(startIndex int32) error {
	s.Log = s.Log[:startIndex]
	if err := s.storage.TrimLog(startIndex); err != nil {
		return fmt.Errorf("failed to save state after trimming log: %w", err)
	}
	return nil
}

func (s *Server) setCommitLength(length int32) error {
	s.CommitLength = length
	if err := s.storage.SaveCommitLength(length); err != nil {
		return fmt.Errorf("failed to save state after setting commit length: %w", err)
	}
	return nil
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

func (s *Server) Broadcast(ctx context.Context, req *pb.BroadcastRequest) (*pb.BroadcastResponse, error) {
	s.logger.Infow("Received broadcast request", "message", req.GetMessage())
	if s.CurrentRole == "leader" {
		s.storage.AppendLog(LogEntry{
			Message: req.GetMessage(),
			Term:    s.CurrentTerm,
		})
		s.AckedLength[s.nodeID] = int32(len(s.Log))
		for peerID := range s.peers {
			if err := s.ReplicateLog(ctx, peerID); err != nil {
				s.logger.Errorw("Failed to replicate log", "error", err)
			}
		}
	} else {
		leaderID := s.VolatileState.CurrentLeader
		leader := s.peers[leaderID]
		leader.Broadcast(ctx, req)
	}
	s.logger.Infow("Acknowledged broadcast request", "message", req.GetMessage())
	return &pb.BroadcastResponse{
		Success: true,
		NodeId:  s.nodeID,
	}, nil
}

func (s *Server) ReplicateLog(ctx context.Context, followerID string) error {
	s.logger.Infow("Replicating log", "follower_id", followerID)
	prefixLen := s.SentLength[followerID]
	suffix := s.Log[prefixLen:]
	prefixTerm := int32(0)
	if prefixLen > 0 {
		prefixTerm = s.Log[prefixLen-1].Term
	}
	// TODO: not very clean, but it works
	pb_suffix := make([]*pb.LogEntry, len(suffix))
	for i, entry := range suffix {
		pb_suffix[i] = &pb.LogEntry{
			Message: entry.Message,
			Term:    entry.Term,
		}
	}
	s.peers[followerID].RequestLog(ctx, &pb.LogRequest{
		LeaderId:     s.nodeID,
		Term:         s.CurrentTerm,
		PrefixLen:    int32(prefixLen),
		PrefixTerm:   int32(prefixTerm),
		CommitLength: s.CommitLength,
		Suffix:       pb_suffix,
	})
	return nil
}

// RaftNodeServer implementation
func (s *Server) RequestVote(ctx context.Context, req *pb.VoteRequest) (*emptypb.Empty, error) {
	s.logger.Infow("Received vote request", "candidate_id", req.CandidateId, "term", req.Term)
	return &emptypb.Empty{}, nil
}

func (s *Server) HandleVoteResponse(ctx context.Context, resp *pb.VoteResponse) (*emptypb.Empty, error) {
	s.logger.Infow("Received vote response", "voter_id", resp.VoterId, "granted", resp.Granted, "term", resp.Term)
	return &emptypb.Empty{}, nil
}

func (s *Server) RequestLog(ctx context.Context, req *pb.LogRequest) (*emptypb.Empty, error) {
	s.logger.Infow("Received log request", "leader_id", req.LeaderId, "term", req.Term)

	if req.Term > s.CurrentTerm {
		s.setCurrentTerm(req.Term)
		s.setVotedFor("")
		//TODO: cancel election timer
	}
	if req.Term == s.CurrentTerm {
		s.CurrentRole = "follower"
		s.CurrentLeader = req.LeaderId
	}
	logOk := len(s.Log) >= int(req.PrefixLen) && (req.PrefixLen == 0 || s.Log[req.PrefixLen-1].Term == req.PrefixTerm)
	if req.Term == s.CurrentTerm && logOk {
		s.AppendEntries(req.PrefixLen, req.CommitLength, req.Suffix)
		ack := req.PrefixLen + int32(len(req.Suffix))
		s.peers[req.LeaderId].HandleLogResponse(ctx, &pb.LogResponse{
			FollowerId: s.nodeID,
			Term:       s.CurrentTerm,
			Ack:        ack,
			Success:    true,
		})
	} else {
		s.peers[req.LeaderId].HandleLogResponse(ctx, &pb.LogResponse{
			FollowerId: s.nodeID,
			Term:       s.CurrentTerm,
			Ack:        0,
			Success:    false,
		})
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) AppendEntries(prefixLen int32, leaderCommitLength int32, suffix []*pb.LogEntry) error {
	s.logger.Infow("Appending entries", "prefix_len", prefixLen, "leader_commit_length", leaderCommitLength, "suffix", suffix)
	return nil
}

func (s *Server) HandleLogResponse(ctx context.Context, resp *pb.LogResponse) (*emptypb.Empty, error) {
	s.logger.Infow("Received log response", "follower_id", resp.FollowerId, "success", resp.Success, "term", resp.Term)
	return &emptypb.Empty{}, nil
}

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
