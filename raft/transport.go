package raft

import "context"

type Transport interface {
	SendBroadcastRequest(ctx context.Context, nodeID string, req *BroadcastRequest) (*BroadcastResponse, error)
	SendLogRequest(nodeID string, req *LogRequest)
	SendLogResponse(nodeID string, req *LogResponse)
	SendVoteRequest(nodeID string, req *VoteRequest)
	SendVoteResponse(nodeID string, req *VoteResponse)
}
