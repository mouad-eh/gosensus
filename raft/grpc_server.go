package raft

import (
	"fmt"
	"net"
	"os"

	pb "github.com/mouad-eh/gosensus/rpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCServerConfig struct {
	IP         net.IP
	NodePort   int
	ClientPort int
	Leader     *net.TCPAddr
	Peers      []*net.TCPAddr
}

type NodeEndpoint struct {
	IP       net.IP
	NodePort int
}

func (n *NodeEndpoint) String() string {
	return fmt.Sprintf("%s:%d", n.IP.String(), n.NodePort)
}

type ClientEndpoint struct {
	IP         net.IP
	ClientPort int
}

func (c *ClientEndpoint) String() string {
	return fmt.Sprintf("%s:%d", c.IP.String(), c.ClientPort)
}

type gRPCServer struct {
	config *GRPCServerConfig
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

// NewgRPCServer creates a new Raft server instance
func NewgRPCServer(config *GRPCServerConfig) *gRPCServer {
	nodeAddr := net.TCPAddr{
		IP:   config.IP,
		Port: config.NodePort,
	}
	nodeID := generateNodeID(nodeAddr.String())
	leaderID := generateNodeID(config.Leader.String())
	currentRole := "follower"
	if leaderID == nodeID {
		currentRole = "leader"
	}

	return &gRPCServer{
		config: config,
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

func (s *gRPCServer) Run() error {
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
		s.SentLength[peerID] = 0
		s.AckedLength[peerID] = 0
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
