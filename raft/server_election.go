package raft

import (
	"context"

	pb "github.com/mouad-eh/gosensus/rpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// RaftNodeServer implementation
func (s *Server) RequestVote(ctx context.Context, req *pb.VoteRequest) (*emptypb.Empty, error) {
	s.logger.Infow("Received vote request", "candidate_id", req.CandidateId, "term", req.Term)
	return &emptypb.Empty{}, nil
}

func (s *Server) HandleVoteResponse(ctx context.Context, resp *pb.VoteResponse) (*emptypb.Empty, error) {
	s.logger.Infow("Received vote response", "voter_id", resp.VoterId, "granted", resp.Granted, "term", resp.Term)
	return &emptypb.Empty{}, nil
}
