package raft

import (
	"context"
)

type Raft interface {
	Init() error
	Broadcast(ctx context.Context, req *BroadcastRequest) (*BroadcastResponse, error)
	RequestVote(req *VoteRequest) error
	HandleVoteResponse(req *VoteResponse) error
	RequestLog(req *LogRequest) error
	HandleLogResponse(req *LogResponse) error
}

type BroadcastRequest struct {
	Message string
}

type BroadcastResponse struct {
	LeaderId string
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
