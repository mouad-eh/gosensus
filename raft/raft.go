package raft

import (
	"context"
)

type Raft interface {
	Init(ctx context.Context, role string) error
	Broadcast(ctx context.Context, req *BroadcastRequest) (*BroadcastResponse, error)
	RequestVote(ctx context.Context, req *VoteRequest) error
	HandleVoteResponse(ctx context.Context, req *VoteResponse) error
	RequestLog(ctx context.Context, req *LogRequest) error
	HandleLogResponse(ctx context.Context, req *LogResponse) error
}

type BroadcastRequest struct {
	Message string
}

type BroadcastResponse struct {
	Success bool
	NodeId  string
}

type VoteRequest struct {
	CandidateId string
	Term        int
	LogLength   int
	LogTerm     int
}

type VoteResponse struct {
	VoterId string
	Term    int
	Granted bool
}

type LogRequest struct {
	LeaderId     string
	Term         int
	PrefixLen    int
	PrefixTerm   int
	CommitLength int
	Suffix       []*LogEntry
}

type LogEntry struct {
	Message string
	Term    int
}

type LogResponse struct {
	FollowerId string
	Term       int
	Ack        int
	Success    bool
}
