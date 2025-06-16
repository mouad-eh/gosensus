package raft

import "context"

type Transport interface {
	SendBroadcastRequest(ctx context.Context, nodeID string, req *BroadcastRequest) (*BroadcastResponse, error)
	SendLogRequest(ctx context.Context, nodeID string, req *LogRequest) error
	SendLogResponse(ctx context.Context, nodeID string, req *LogResponse) error
	SendVoteRequest(ctx context.Context, nodeID string, req *VoteRequest) error
	SendVoteResponse(ctx context.Context, nodeID string, req *VoteResponse) error
}
