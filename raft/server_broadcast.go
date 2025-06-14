package raft

import (
	"context"

	pb "github.com/mouad-eh/gosensus/rpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (s *Server) Broadcast(ctx context.Context, req *pb.BroadcastRequest) (*pb.BroadcastResponse, error) {
	s.logger.Infow("Received broadcast request", "message", req.GetMessage())
	if s.CurrentRole == "leader" {
		s.appendLog(LogEntry{
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
	// block until the message is delivered
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

	go func() {
		// Create a new background context for the goroutine
		bgCtx := context.Background()
		_, err := s.peers[followerID].RequestLog(bgCtx, &pb.LogRequest{
			LeaderId:     s.nodeID,
			Term:         s.CurrentTerm,
			PrefixLen:    int32(prefixLen),
			PrefixTerm:   int32(prefixTerm),
			CommitLength: s.CommitLength,
			Suffix:       pb_suffix,
		})
		if err != nil {
			s.logger.Errorw("Failed to replicate log", "error", err, "follower_id", followerID)
		}
	}()
	return nil
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
		go func() {
			bgCtx := context.Background()
			_, err := s.peers[req.LeaderId].HandleLogResponse(bgCtx, &pb.LogResponse{
				FollowerId: s.nodeID,
				Term:       s.CurrentTerm,
				Ack:        ack,
				Success:    true,
			})
			if err != nil {
				s.logger.Errorw("Failed to send log response", "error", err, "leader_id", req.LeaderId)
			}
		}()
	} else {
		go func() {
			bgCtx := context.Background()
			_, err := s.peers[req.LeaderId].HandleLogResponse(bgCtx, &pb.LogResponse{
				FollowerId: s.nodeID,
				Term:       s.CurrentTerm,
				Ack:        0,
				Success:    false,
			})
			if err != nil {
				s.logger.Errorw("Failed to send log response", "error", err, "leader_id", req.LeaderId)
			}
		}()
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) AppendEntries(prefixLen int32, leaderCommitLength int32, suffix []*pb.LogEntry) error {
	s.logger.Infow("Appending entries", "prefix_len", prefixLen, "leader_commit_length", leaderCommitLength, "suffix", suffix)
	if len(suffix) > 0 && len(s.Log) > int(prefixLen) {
		index := min(len(s.Log), int(prefixLen)+len(suffix))
		if s.Log[index].Term != suffix[index-int(prefixLen)].Term {
			s.trimLog(prefixLen)
		}
	}
	if int(prefixLen)+len(suffix) > len(s.Log) {
		for i := len(s.Log) - int(prefixLen); i < len(suffix); i++ {
			s.appendLog(LogEntry{
				Message: suffix[i].Message,
				Term:    suffix[i].Term,
			})
		}
	}
	if leaderCommitLength > s.CommitLength {
		for i := s.CommitLength; i < leaderCommitLength; i++ {
			s.logger.Infow("Committing log", "index", i, "message", s.Log[i].Message)
		}
		s.setCommitLength(leaderCommitLength)
	}
	return nil
}

func (s *Server) HandleLogResponse(ctx context.Context, resp *pb.LogResponse) (*emptypb.Empty, error) {
	s.logger.Infow("Received log response", "follower_id", resp.FollowerId, "success", resp.Success, "term", resp.Term)
	if resp.Term == s.CurrentTerm && s.CurrentRole == "leader" {
		if resp.Success && resp.Ack > s.AckedLength[resp.FollowerId] {
			s.SentLength[resp.FollowerId] = resp.Ack
			s.AckedLength[resp.FollowerId] = resp.Ack
			s.CommitLogEntries()
		} else if s.SentLength[resp.FollowerId] > 0 {
			s.SentLength[resp.FollowerId] = s.SentLength[resp.FollowerId] - 1
			s.ReplicateLog(ctx, resp.FollowerId)
		}
	} else if resp.Term > s.CurrentTerm {
		s.setCurrentTerm(resp.Term)
		s.CurrentRole = "follower"
		s.setVotedFor("")
		//TODO: cancel election timer
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) acks(length int32) int {
	cpt := 0
	for node := range s.AckedLength {
		if s.AckedLength[node] >= length {
			cpt++
		}
	}
	return cpt
}

// Executed by leader only
func (s *Server) CommitLogEntries() {
	numNodes := len(s.AckedLength)
	minAcks := (numNodes + 1) / 2
	ready := make(map[int]struct{})
	for i := 1; i <= len(s.Log); i++ {
		if s.acks(int32(i)) >= minAcks {
			ready[i] = struct{}{}
		}
	}

	var maxReady int
	for k := range ready {
		if k > maxReady {
			maxReady = k
		}
	}

	if len(ready) > 0 && maxReady > int(s.CommitLength) && s.Log[maxReady-1].Term == s.CurrentTerm {
		for i := s.CommitLength; i <= int32(maxReady)-1; i++ {
			s.logger.Infow("Delivering log", "index", i, "message", s.Log[i].Message)
			// send signal to hanging broadcast RPCs
		}
		s.setCommitLength(int32(maxReady))
	}
}
