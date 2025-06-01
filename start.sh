#!/bin/bash

# Cleanup old state
rm -rf state 2>/dev/null

# Kill any existing tmux session
tmux kill-session -t raft 2>/dev/null

# Create a new tmux session
tmux new-session -d -s raft

# Split the window into a 2x2 grid
tmux split-window -h
tmux split-window -v
tmux select-pane -t 0
tmux split-window -v

# Start node 1 (top-right)
tmux send-keys -t raft:0.1 "go run main.go --ip localhost --client-port 8001 --node-port 9001 --peers localhost:9002,localhost:9003 --leader localhost:9001" C-m

# Start node 2 (bottom-left)
tmux send-keys -t raft:0.2 "go run main.go --ip localhost --client-port 8002 --node-port 9002 --peers localhost:9001,localhost:9003 --leader localhost:9001" C-m

# Start node 3 (bottom-right)
tmux send-keys -t raft:0.3 "go run main.go --ip localhost --client-port 8003 --node-port 9003 --peers localhost:9001,localhost:9002 --leader localhost:9001" C-m

# Client (top-left)
tmux send-keys -t raft:0.0 "go run client/client.go --node localhost:8001 --message 'hi'" 

# Attach to the tmux session
tmux attach-session -t raft
