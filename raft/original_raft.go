package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	persistence "github.com/mouad-eh/gosensus/raft/persistence"
	"go.uber.org/zap"
)

type OriginalRaft struct {
	nodeID string
	peers  []string
	// Persistent State
	CurrentTerm  int
	VotedFor     string
	Log          []LogEntry
	CommitLength int
	// Volatile State
	CurrentRole   string
	CurrentLeader string
	VotesReceived map[string]struct{}
	SentLength    map[string]int
	AckedLength   map[string]int
	// Transport
	transport Transport
	// Storage
	storage persistence.Storage
	// Logger
	logger *zap.SugaredLogger
	// Delivered log channel
	delivered map[int]chan struct{}
	// Mutex for thread safety
	mu sync.RWMutex
	// Heartbeat timer
	heartbeatTimer *time.Timer
	// // Election timer
	// electionTimer *time.Timer
	// Election timer cancel function
	electionTimerCancel context.CancelFunc
	// Election context
	electionTimerCtx context.Context
}

func NewOriginalRaft(nodeID string, peers []string, transport Transport, storage persistence.Storage, logger *zap.SugaredLogger) *OriginalRaft {
	return &OriginalRaft{
		nodeID:         nodeID,
		peers:          peers,
		transport:      transport,
		storage:        storage,
		logger:         logger,
		delivered:      make(map[int]chan struct{}),
		heartbeatTimer: time.NewTimer(time.Duration(25+rand.Intn(20)) * time.Second),
	}
}

func (raft *OriginalRaft) setCurrentTerm(term int) error {
	raft.CurrentTerm = term
	if err := raft.storage.SaveCurrentTerm(term); err != nil {
		return fmt.Errorf("failed to save state after setting term: %w", err)
	}
	return nil
}

func (raft *OriginalRaft) setVotedFor(votedFor string) error {
	raft.VotedFor = votedFor
	if err := raft.storage.SaveVotedFor(votedFor); err != nil {
		return fmt.Errorf("failed to save state after setting votedFor: %w", err)
	}
	return nil
}

func (raft *OriginalRaft) appendLog(entry LogEntry) (int, error) {
	raft.Log = append(raft.Log, entry)
	if err := raft.storage.AppendLog(persistence.LogEntry{
		Message: entry.Message,
		Term:    entry.Term,
	}); err != nil {
		return 0, fmt.Errorf("failed to save state after appending log: %w", err)
	}
	return len(raft.Log) - 1, nil
}

func (raft *OriginalRaft) trimLog(startIndex int) error {
	raft.Log = raft.Log[:startIndex]
	if err := raft.storage.TrimLog(startIndex); err != nil {
		return fmt.Errorf("failed to save state after trimming log: %w", err)
	}
	return nil
}

func (raft *OriginalRaft) setCommitLength(length int) error {
	raft.logger.Infow("Setting commit length", "length", length)
	raft.CommitLength = length
	if err := raft.storage.SaveCommitLength(length); err != nil {
		return fmt.Errorf("failed to save state after setting commit length: %w", err)
	}
	return nil
}

func (raft *OriginalRaft) Init(role string) error {
	if err := raft.storage.Init(raft.nodeID); err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}
	// load persistent state
	currentTerm, votedFor, commitLength, log, err := raft.storage.LoadState()
	if err != nil {
		return fmt.Errorf("failed to load state from storage: %w", err)
	}
	raft.CurrentTerm = currentTerm
	raft.VotedFor = votedFor
	raft.CommitLength = commitLength
	// Convert persistence.LogEntry to LogEntry
	raft.Log = make([]LogEntry, len(log))
	for i, entry := range log {
		raft.Log[i] = LogEntry{
			Message: entry.Message,
			Term:    entry.Term,
		}
	}
	// Initialize volatile state
	raft.CurrentRole = role
	raft.CurrentLeader = ""
	raft.VotesReceived = make(map[string]struct{})
	raft.SentLength = make(map[string]int)
	raft.AckedLength = make(map[string]int)
	// Start periodic replication
	ticker := time.NewTicker(10 * time.Second)
	go raft.PeriodicReplicateLog(ticker.C)
	// Start heartbeat timer
	go raft.CheckLeaderFailure()
	// Start election timer
	raft.electionTimerCtx, raft.electionTimerCancel = context.WithCancel(context.Background())

	return nil
}

func (raft *OriginalRaft) CheckLeaderFailure() error {
	<-raft.heartbeatTimer.C
	if raft.CurrentRole == "follower" {
		raft.logger.Infow("Heartbeat timeout")
		err := raft.StartElection()
		if err != nil {
			return fmt.Errorf("failed to start election: %w", err)
		}
	}
	return nil
}

func (raft *OriginalRaft) PeriodicReplicateLog(ch <-chan time.Time) {
	for {
		<-ch
		if raft.CurrentRole == "leader" {
			raft.logger.Infow("Periodic log replication by leader")
			for _, peerID := range raft.peers {
				raft.ReplicateLog(peerID)
			}
		}
	}
}

func (raft *OriginalRaft) Broadcast(ctx context.Context, req *BroadcastRequest) (*BroadcastResponse, error) {
	raft.logger.Infow("Received broadcast request", "message", req.Message)
	if raft.CurrentRole == "leader" {
		index, err := raft.appendLog(LogEntry{
			Message: req.Message,
			Term:    raft.CurrentTerm,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to append log: %w", err)
		}
		raft.AckedLength[raft.nodeID] = len(raft.Log)
		raft.delivered[index] = make(chan struct{})
		for _, peerID := range raft.peers {
			raft.ReplicateLog(peerID)
		}
		// block until the message is delivered
		select {
		case <-raft.delivered[index]:
			delete(raft.delivered, index)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		raft.logger.Infow("Acknowledged broadcast request", "message", req.Message)
		return &BroadcastResponse{
			Success: true,
			NodeId:  raft.nodeID,
		}, nil
	} else {
		leaderID := raft.CurrentLeader
		resp, err := raft.transport.SendBroadcastRequest(ctx, leaderID, req)
		if err != nil {
			raft.logger.Errorw("Failed to forward broadcast request to leader", "error", err)
			return nil, fmt.Errorf("failed to forward broadcast request to leader: %w", err)
		}
		raft.logger.Infow("Acknowledged broadcast request", "message", req.Message)
		return resp, nil
	}
}

func (raft *OriginalRaft) ReplicateLog(followerID string) {
	raft.logger.Infow("Replicating log", "follower_id", followerID)
	prefixLen := raft.SentLength[followerID]
	suffix := raft.Log[prefixLen:]
	prefixTerm := 0
	if prefixLen > 0 {
		prefixTerm = raft.Log[prefixLen-1].Term
	}

	// Convert suffix to LogEntry slice
	logSuffix := make([]*LogEntry, len(suffix))
	for i, entry := range suffix {
		logSuffix[i] = &LogEntry{
			Message: entry.Message,
			Term:    entry.Term,
		}
	}

	raft.transport.SendLogRequest(followerID, &LogRequest{
		LeaderId:     raft.nodeID,
		Term:         raft.CurrentTerm,
		PrefixLen:    prefixLen,
		PrefixTerm:   prefixTerm,
		CommitLength: raft.CommitLength,
		Suffix:       logSuffix,
	})
}

func (raft *OriginalRaft) RequestLog(req *LogRequest) error {
	raft.logger.Infow("Received log request", "leader_id", req.LeaderId, "term", req.Term)
	raft.heartbeatTimer.Reset(time.Duration(25+rand.Intn(10)) * time.Second)
	if req.Term > raft.CurrentTerm {
		err := raft.setCurrentTerm(req.Term)
		if err != nil {
			return fmt.Errorf("failed to set current term: %w", err)
		}
		err = raft.setVotedFor("")
		if err != nil {
			return fmt.Errorf("failed to set voted for: %w", err)
		}
		raft.CancelElectionTimer()
	}
	if req.Term == raft.CurrentTerm {
		if raft.CurrentRole != "follower" {
			go raft.CheckLeaderFailure()
		}
		raft.CurrentRole = "follower"
		raft.CurrentLeader = req.LeaderId
	}
	logOk := len(raft.Log) >= req.PrefixLen && (req.PrefixLen == 0 || raft.Log[req.PrefixLen-1].Term == req.PrefixTerm)
	if req.Term == raft.CurrentTerm && logOk {
		err := raft.AppendEntries(req.PrefixLen, req.CommitLength, req.Suffix)
		if err != nil {
			return fmt.Errorf("failed to append entries: %w", err)
		}
		ack := req.PrefixLen + len(req.Suffix)
		raft.transport.SendLogResponse(req.LeaderId, &LogResponse{
			FollowerId: raft.nodeID,
			Term:       raft.CurrentTerm,
			Ack:        ack,
			Success:    true,
		})
	} else {
		raft.transport.SendLogResponse(req.LeaderId, &LogResponse{
			FollowerId: raft.nodeID,
			Term:       raft.CurrentTerm,
			Ack:        0,
			Success:    false,
		})
	}
	return nil
}

func (raft *OriginalRaft) AppendEntries(prefixLen int, leaderCommitLength int, suffix []*LogEntry) error {
	raft.logger.Infow("Appending entries", "prefix_len", prefixLen, "leader_commit_length", leaderCommitLength, "suffix", suffix)
	if len(suffix) > 0 && len(raft.Log) > prefixLen {
		index := min(len(raft.Log), prefixLen+len(suffix))
		if raft.Log[index].Term != suffix[index-prefixLen].Term {
			err := raft.trimLog(prefixLen)
			if err != nil {
				return fmt.Errorf("failed to trim log: %w", err)
			}
		}
	}
	if prefixLen+len(suffix) > len(raft.Log) {
		for i := len(raft.Log) - prefixLen; i < len(suffix); i++ {
			_, err := raft.appendLog(LogEntry{
				Message: suffix[i].Message,
				Term:    suffix[i].Term,
			})
			if err != nil {
				return fmt.Errorf("failed to append log: %w", err)
			}
		}
	}
	if leaderCommitLength > raft.CommitLength {
		for i := raft.CommitLength; i < leaderCommitLength; i++ {
			raft.logger.Infow("Delivering log", "index", i, "message", raft.Log[i].Message)
		}
		err := raft.setCommitLength(leaderCommitLength)
		if err != nil {
			return fmt.Errorf("failed to set commit length: %w", err)
		}
	}
	return nil
}

func (raft *OriginalRaft) HandleLogResponse(resp *LogResponse) error {
	raft.logger.Infow("Received log response", "follower_id", resp.FollowerId, "success", resp.Success, "term", resp.Term, "ack", resp.Ack)
	if resp.Term == raft.CurrentTerm && raft.CurrentRole == "leader" {
		if resp.Success && resp.Ack >= raft.AckedLength[resp.FollowerId] {
			raft.SentLength[resp.FollowerId] = resp.Ack
			raft.AckedLength[resp.FollowerId] = resp.Ack
			err := raft.CommitLogEntries()
			if err != nil {
				return fmt.Errorf("failed to commit log entries: %w", err)
			}
		} else if raft.SentLength[resp.FollowerId] > 0 {
			raft.SentLength[resp.FollowerId] = raft.SentLength[resp.FollowerId] - 1
			raft.ReplicateLog(resp.FollowerId)
		}
	} else if resp.Term > raft.CurrentTerm {
		err := raft.setCurrentTerm(resp.Term)
		if err != nil {
			return fmt.Errorf("failed to set current term: %w", err)
		}
		err = raft.setVotedFor("")
		if err != nil {
			return fmt.Errorf("failed to set voted for: %w", err)
		}
		raft.CurrentRole = "follower"
		go raft.CheckLeaderFailure()
		raft.CancelElectionTimer()
	}
	return nil
}

func (raft *OriginalRaft) acks(length int) int {
	cpt := 0
	for node := range raft.AckedLength {
		if raft.AckedLength[node] >= length {
			cpt++
		}
	}
	return cpt
}

// Executed by leader only
func (raft *OriginalRaft) CommitLogEntries() error {
	numNodes := len(raft.AckedLength)
	minAcks := (numNodes + 1) / 2
	ready := make([]int, 0)
	for i := 1; i <= len(raft.Log); i++ {
		if raft.acks(i) >= minAcks {
			ready = append(ready, i)
		}
	}

	var maxReady int
	for _, k := range ready {
		if k > maxReady {
			maxReady = k
		}
	}

	// The lock is used to make the read and the write to raft.CommitLength below atomic
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if len(ready) > 0 && maxReady > raft.CommitLength && raft.Log[maxReady-1].Term == raft.CurrentTerm {

		for i := raft.CommitLength; i <= maxReady-1; i++ {
			raft.logger.Infow("Delivering log", "index", i, "message", raft.Log[i].Message)
			// send signal to hanging broadcast RPCs
			raft.delivered[i] <- struct{}{}
			close(raft.delivered[i])
		}
		err := raft.setCommitLength(maxReady)
		if err != nil {
			return fmt.Errorf("failed to set commit length: %w", err)
		}
	}
	return nil
}

func (raft *OriginalRaft) StartElection() error {
	raft.logger.Infow("Starting election", "current_term", raft.CurrentTerm)
	err := raft.setCurrentTerm(raft.CurrentTerm + 1)
	if err != nil {
		return fmt.Errorf("failed to set current term: %w", err)
	}
	raft.CurrentRole = "candidate"
	err = raft.setVotedFor(raft.nodeID)
	if err != nil {
		return fmt.Errorf("failed to set voted for: %w", err)
	}
	raft.VotesReceived[raft.nodeID] = struct{}{}
	lastTerm := 0
	if len(raft.Log) > 0 {
		lastTerm = raft.Log[len(raft.Log)-1].Term
	}
	// send to peers
	for _, peerID := range raft.peers {
		raft.logger.Infow("Sending vote request to peer", "peer_id", peerID, "candidate_id", raft.nodeID, "term", raft.CurrentTerm, "log_length", len(raft.Log), "log_term", lastTerm)
		raft.transport.SendVoteRequest(peerID, &VoteRequest{
			CandidateId: raft.nodeID,
			Term:        raft.CurrentTerm,
			LogLength:   len(raft.Log),
			LogTerm:     lastTerm,
		})
	}
	// send to himself
	raft.logger.Infow("Sending vote request to himself", "candidate_id", raft.nodeID, "term", raft.CurrentTerm, "log_length", len(raft.Log), "log_term", lastTerm)
	raft.transport.SendVoteRequest(raft.nodeID, &VoteRequest{
		CandidateId: raft.nodeID,
		Term:        raft.CurrentTerm,
		LogLength:   len(raft.Log),
		LogTerm:     lastTerm,
	})
	raft.StartElectionTimer()
	return nil
}

func (raft *OriginalRaft) StartElectionTimer() {
	electionTimer := time.NewTimer(time.Duration(25+rand.Intn(20)) * time.Second)
	go func() {
		select {
		case <-electionTimer.C:
			raft.logger.Infow("Election timer expired")
			err := raft.StartElection()
			if err != nil {
				raft.logger.Errorw("Failed to start election", "error", err)
			}
		case <-raft.electionTimerCtx.Done():
			electionTimer.Stop()
			return
		}
	}()
}

func (raft *OriginalRaft) CancelElectionTimer() {
	raft.electionTimerCancel()
	raft.electionTimerCtx, raft.electionTimerCancel = context.WithCancel(context.Background())
}

func (raft *OriginalRaft) RequestVote(req *VoteRequest) error {
	raft.logger.Infow("Received vote request", "candidate_id", req.CandidateId, "term", req.Term, "log_length", req.LogLength, "log_term", req.LogTerm)
	if req.Term > raft.CurrentTerm {
		err := raft.setCurrentTerm(req.Term)
		if err != nil {
			return fmt.Errorf("failed to set current term: %w", err)
		}
		raft.CurrentRole = "follower"
		go raft.CheckLeaderFailure()
		err = raft.setVotedFor("")
		if err != nil {
			return fmt.Errorf("failed to set voted for: %w", err)
		}
	}
	lastTerm := 0
	if len(raft.Log) > 0 {
		lastTerm = raft.Log[len(raft.Log)-1].Term
	}
	logOk := (req.LogTerm > lastTerm) || (req.LogTerm == lastTerm && req.LogLength >= len(raft.Log))
	if req.Term == raft.CurrentTerm && logOk && (raft.VotedFor == "" || raft.VotedFor == req.CandidateId) {
		err := raft.setVotedFor(req.CandidateId)
		if err != nil {
			return fmt.Errorf("failed to set voted for: %w", err)
		}
		raft.logger.Infow("Sending vote response to candidate", "candidate_id", req.CandidateId, "voter_id", raft.nodeID, "term", raft.CurrentTerm, "granted", true)
		raft.transport.SendVoteResponse(req.CandidateId, &VoteResponse{
			VoterId: raft.nodeID,
			Term:    raft.CurrentTerm,
			Granted: true,
		})
	} else {
		raft.transport.SendVoteResponse(req.CandidateId, &VoteResponse{
			VoterId: raft.nodeID,
			Term:    raft.CurrentTerm,
			Granted: false,
		})
	}
	return nil
}

func (raft *OriginalRaft) HandleVoteResponse(resp *VoteResponse) error {
	raft.logger.Infow("Received vote response", "voter_id", resp.VoterId, "term", resp.Term, "granted", resp.Granted)
	if raft.CurrentRole == "candidate" && raft.CurrentTerm == resp.Term && resp.Granted {
		raft.VotesReceived[resp.VoterId] = struct{}{}
		numNodes := len(raft.peers) + 1
		if len(raft.VotesReceived) >= (numNodes+1)/2 {
			raft.logger.Infow("Received enough votes to become leader")
			raft.CurrentRole = "leader"
			raft.CurrentLeader = raft.nodeID
			raft.CancelElectionTimer()
			for _, peerID := range raft.peers {
				raft.SentLength[peerID] = len(raft.Log)
				raft.AckedLength[peerID] = 0
				raft.ReplicateLog(peerID)
			}
		}
	} else if resp.Term > raft.CurrentTerm {
		err := raft.setCurrentTerm(resp.Term)
		if err != nil {
			return fmt.Errorf("failed to set current term: %w", err)
		}
		raft.CurrentRole = "follower"
		go raft.CheckLeaderFailure()
		err = raft.setVotedFor("")
		if err != nil {
			return fmt.Errorf("failed to set voted for: %w", err)
		}
		raft.CancelElectionTimer()
	}
	return nil
}
