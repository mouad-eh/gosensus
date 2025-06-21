package raft

import (
	"context"
	"fmt"

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
}

func NewOriginalRaft(nodeID string, peers []string, transport Transport, storage persistence.Storage, logger *zap.SugaredLogger) *OriginalRaft {
	return &OriginalRaft{
		nodeID:    nodeID,
		peers:     peers,
		transport: transport,
		storage:   storage,
		logger:    logger,
		delivered: make(map[int]chan struct{}),
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

func (raft *OriginalRaft) Init(ctx context.Context, role string) error {
	if err := raft.storage.Init(raft.nodeID); err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}
	// load persistent state
	currentTerm, votedFor, commitLength, log, err := raft.storage.LoadState()
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
	if err != nil {
		return fmt.Errorf("failed to load state from storage: %w", err)
	}
	return nil
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
			raft.ReplicateLog(ctx, peerID)
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

func (raft *OriginalRaft) ReplicateLog(ctx context.Context, followerID string) {
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

func (raft *OriginalRaft) RequestLog(ctx context.Context, req *LogRequest) error {
	raft.logger.Infow("Received log request", "leader_id", req.LeaderId, "term", req.Term)

	if req.Term > raft.CurrentTerm {
		err := raft.setCurrentTerm(req.Term)
		if err != nil {
			return fmt.Errorf("failed to set current term: %w", err)
		}
		err = raft.setVotedFor("")
		if err != nil {
			return fmt.Errorf("failed to set voted for: %w", err)
		}
		//TODO: cancel election timer
	}
	if req.Term == raft.CurrentTerm {
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
			raft.logger.Infow("Committing log", "index", i, "message", raft.Log[i].Message)
		}
		err := raft.setCommitLength(leaderCommitLength)
		if err != nil {
			return fmt.Errorf("failed to set commit length: %w", err)
		}
	}
	return nil
}

func (raft *OriginalRaft) HandleLogResponse(ctx context.Context, resp *LogResponse) error {
	raft.logger.Infow("Received log response", "follower_id", resp.FollowerId, "success", resp.Success, "term", resp.Term)
	if resp.Term == raft.CurrentTerm && raft.CurrentRole == "leader" {
		if resp.Success && resp.Ack > raft.AckedLength[resp.FollowerId] {
			raft.SentLength[resp.FollowerId] = resp.Ack
			raft.AckedLength[resp.FollowerId] = resp.Ack
			err := raft.CommitLogEntries()
			if err != nil {
				return fmt.Errorf("failed to commit log entries: %w", err)
			}
		} else if raft.SentLength[resp.FollowerId] > 0 {
			raft.SentLength[resp.FollowerId] = raft.SentLength[resp.FollowerId] - 1
			raft.ReplicateLog(ctx, resp.FollowerId)
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
		//TODO: cancel election timer
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
	ready := make(map[int]struct{})
	for i := 1; i <= len(raft.Log); i++ {
		if raft.acks(i) >= minAcks {
			ready[i] = struct{}{}
		}
	}

	var maxReady int
	for k := range ready {
		if k > maxReady {
			maxReady = k
		}
	}

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

func (raft *OriginalRaft) RequestVote(ctx context.Context, req *VoteRequest) error {
	raft.logger.Infow("Received vote request", "candidate_id", req.CandidateId, "term", req.Term)
	// TODO: implement
	return nil
}

func (raft *OriginalRaft) HandleVoteResponse(ctx context.Context, resp *VoteResponse) error {
	raft.logger.Infow("Received vote response", "voter_id", resp.VoterId, "granted", resp.Granted, "term", resp.Term)
	// TODO: implement
	return nil
}
