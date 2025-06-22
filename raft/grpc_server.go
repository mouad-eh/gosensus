package raft

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/mouad-eh/gosensus/raft/persistence"
	pb "github.com/mouad-eh/gosensus/rpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type GRPCServerConfig struct {
	IP         net.IP
	NodePort   int
	ClientPort int
	Leader     *net.TCPAddr
	Peers      []*net.TCPAddr
}

type gRPCServer struct {
	raft   Raft
	config *GRPCServerConfig
	pb.UnimplementedRaftClientServer
	pb.UnimplementedRaftNodeServer
	nodeID string
	// Map of node ID to RaftNodeClient
	peers map[string]pb.RaftNodeClient
	// Logger
	logger *zap.SugaredLogger
}

// NewgRPCServer creates a new Raft server instance
func NewgRPCServer(config *GRPCServerConfig) *gRPCServer {
	nodeAddr := net.TCPAddr{
		IP:   config.IP,
		Port: config.NodePort,
	}
	nodeID := generateNodeID(nodeAddr.String())

	return &gRPCServer{
		config: config,
		raft:   nil,
		nodeID: nodeID,
		peers:  make(map[string]pb.RaftNodeClient),
		logger: NewSugaredZapLogger(nodeID),
	}
}

func (s *gRPCServer) Run() error {
	peers := make([]string, len(s.config.Peers))
	for i, peer := range s.config.Peers {
		peers[i] = generateNodeID(peer.String())
	}
	s.raft = NewOriginalRaft(s.nodeID, peers, s, persistence.NewJSONStorage(s.nodeID), s.logger)
	// determine role
	role := "follower"
	if s.nodeID == generateNodeID(s.config.Leader.String()) {
		role = "leader"
	}
	// Initialize raft
	if err := s.raft.Init(role); err != nil {
		return fmt.Errorf("failed to initialize raft: %v", err)
	}
	// Connect to all peers
	if err := s.connectToPeers(s.config.Peers); err != nil {
		return fmt.Errorf("failed to connect to peers: %v", err)
	}

	// Start servers
	if err := s.startServers(s.config.ClientPort, s.config.NodePort); err != nil {
		return fmt.Errorf("failed to start servers: %v", err)
	}

	return nil
}

// connectToPeers establishes connections to all peer nodes
// TODO: rename this function
func (s *gRPCServer) connectToPeers(peers []*net.TCPAddr) error {
	for _, peer := range peers {
		peerID := generateNodeID(peer.String())
		conn, err := grpc.NewClient(peer.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to peer %s at %s: %v", peerID, peer.String(), err)
		}
		s.peers[peerID] = pb.NewRaftNodeClient(conn)
		s.logger.Infow("Connected to peer", "peer_id", peerID, "address", peer.String())
	}
	return nil
}

// startServers starts both the client and node gRPCServers
func (s *gRPCServer) startServers(clientPort, nodePort int) error {
	// Start client gRPCServer
	clientListener, err := net.Listen("tcp", fmt.Sprintf(":%d", clientPort))
	if err != nil {
		return fmt.Errorf("error starting client gRPCServer: %v", err)
	}
	defer clientListener.Close()

	s.logger.Infow("Client gRPCServer listening", "port", clientPort)

	clientServer := grpc.NewServer()
	pb.RegisterRaftClientServer(clientServer, s)

	// Start node gRPCServer
	nodeListener, err := net.Listen("tcp", fmt.Sprintf(":%d", nodePort))
	if err != nil {
		return fmt.Errorf("error starting node gRPCServer: %v", err)
	}
	defer nodeListener.Close()

	s.logger.Infow("Node gRPCServer listening", "port", nodePort)

	nodeServer := grpc.NewServer()
	pb.RegisterRaftNodeServer(nodeServer, s)

	// Start both gRPCServers in separate goroutines
	go func() {
		if err := clientServer.Serve(clientListener); err != nil {
			s.logger.Errorw("Failed to serve client gRPCServer", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		if err := nodeServer.Serve(nodeListener); err != nil {
			s.logger.Errorw("Failed to serve node gRPCServer", "error", err)
			os.Exit(1)
		}
	}()

	// Wait indefinitely
	select {}
}

// Broadcast & Log replication
func (s *gRPCServer) Broadcast(ctx context.Context, req *pb.BroadcastRequest) (*pb.BroadcastResponse, error) {
	response, err := s.raft.Broadcast(ctx, &BroadcastRequest{
		Message: req.GetMessage(),
	})
	if err != nil {
		s.logger.Errorw("Failed to broadcast", "error", err)
		return nil, err
	}
	return &pb.BroadcastResponse{
		Success: response.Success,
		NodeId:  response.NodeId,
	}, nil
}

func (s *gRPCServer) RequestLog(ctx context.Context, req *pb.LogRequest) (*emptypb.Empty, error) {
	suffix := make([]*LogEntry, len(req.Suffix))
	for i, entry := range req.Suffix {
		suffix[i] = &LogEntry{
			Message: entry.Message,
			Term:    int(entry.Term),
		}
	}
	err := s.raft.RequestLog(&LogRequest{
		LeaderId:     req.LeaderId,
		Term:         int(req.Term),
		PrefixLen:    int(req.PrefixLen),
		PrefixTerm:   int(req.PrefixTerm),
		CommitLength: int(req.CommitLength),
		Suffix:       suffix,
	})
	if err != nil {
		s.logger.Errorw("Failed to request log", "error", err)
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *gRPCServer) HandleLogResponse(ctx context.Context, resp *pb.LogResponse) (*emptypb.Empty, error) {
	err := s.raft.HandleLogResponse(&LogResponse{
		FollowerId: resp.FollowerId,
		Term:       int(resp.Term),
		Ack:        int(resp.Ack),
		Success:    resp.Success,
	})
	if err != nil {
		s.logger.Errorw("Failed to handle log response", "error", err)
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// Election
func (s *gRPCServer) RequestVote(ctx context.Context, req *pb.VoteRequest) (*emptypb.Empty, error) {
	err := s.raft.RequestVote(&VoteRequest{
		CandidateId: req.CandidateId,
		Term:        int(req.Term),
		LogLength:   int(req.LogLength),
		LogTerm:     int(req.LogTerm),
	})
	if err != nil {
		s.logger.Errorw("Failed to request vote", "error", err)
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *gRPCServer) HandleVoteResponse(ctx context.Context, resp *pb.VoteResponse) (*emptypb.Empty, error) {
	err := s.raft.HandleVoteResponse(&VoteResponse{
		VoterId: resp.VoterId,
		Term:    int(resp.Term),
		Granted: resp.Granted,
	})
	if err != nil {
		s.logger.Errorw("Failed to handle vote response", "error", err)
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// Transport interface implementation
func (s *gRPCServer) SendBroadcastRequest(ctx context.Context, nodeID string, req *BroadcastRequest) (*BroadcastResponse, error) {
	peer := s.peers[nodeID]

	resp, err := peer.Broadcast(ctx, &pb.BroadcastRequest{
		Message: req.Message,
	})
	if err != nil {
		s.logger.Errorw("Failed to send broadcast request", "error", err)
		return nil, err
	}
	return &BroadcastResponse{
		Success: resp.Success,
		NodeId:  resp.NodeId,
	}, nil
}

func (s *gRPCServer) SendLogRequest(nodeID string, req *LogRequest) {
	peer := s.peers[nodeID]

	go func() {
		suffix := make([]*pb.LogEntry, len(req.Suffix))
		for i, entry := range req.Suffix {
			suffix[i] = &pb.LogEntry{
				Message: entry.Message,
				Term:    int32(entry.Term),
			}
		}
		ctx := context.Background()
		_, err := peer.RequestLog(ctx, &pb.LogRequest{
			LeaderId:     req.LeaderId,
			Term:         int32(req.Term),
			PrefixLen:    int32(req.PrefixLen),
			PrefixTerm:   int32(req.PrefixTerm),
			CommitLength: int32(req.CommitLength),
			Suffix:       suffix,
		})
		if err != nil {
			s.logger.Errorw("Failed to send log request", "error", err)
		}
	}()
}

func (s *gRPCServer) SendLogResponse(nodeID string, req *LogResponse) {
	peer := s.peers[nodeID]

	go func() {
		ctx := context.Background()
		_, err := peer.HandleLogResponse(ctx, &pb.LogResponse{
			FollowerId: req.FollowerId,
			Term:       int32(req.Term),
			Ack:        int32(req.Ack),
			Success:    req.Success,
		})
		if err != nil {
			s.logger.Errorw("Failed to send log response", "error", err)
		}
	}()
}

func (s *gRPCServer) SendVoteRequest(nodeID string, req *VoteRequest) {
	peer := s.peers[nodeID]

	go func() {
		ctx := context.Background()
		_, err := peer.RequestVote(ctx, &pb.VoteRequest{
			CandidateId: req.CandidateId,
			Term:        int32(req.Term),
			LogLength:   int32(req.LogLength),
			LogTerm:     int32(req.LogTerm),
		})
		if err != nil {
			s.logger.Errorw("Failed to send vote request", "error", err)
		}
	}()
}

func (s *gRPCServer) SendVoteResponse(nodeID string, req *VoteResponse) {
	peer := s.peers[nodeID]

	go func() {
		ctx := context.Background()
		_, err := peer.HandleVoteResponse(ctx, &pb.VoteResponse{
			VoterId: req.VoterId,
			Term:    int32(req.Term),
			Granted: req.Granted,
		})
		if err != nil {
			s.logger.Errorw("Failed to send vote response", "error", err)
		}
	}()
}
