#!/bin/bash

# Cleanup old state
rm -rf state 2>/dev/null

# Kill any existing tmux session
tmux kill-session -t raft 2>/dev/null

# Create a new tmux session
tmux new-session -d -s raft

# Enable mouse mode and set terminal options
tmux set -g mouse on
tmux set -g terminal-overrides 'xterm*:smcup@:rmcup@'

# Create windows for each component
tmux new-window -t raft:1 -n 'node1'
tmux new-window -t raft:2 -n 'node2'
tmux new-window -t raft:3 -n 'node3'
tmux new-window -t raft:4 -n 'client'

# Start node 1
tmux send-keys -t raft:1 "go run main.go --ip localhost --client-port 8001 --node-port 9001 --peers localhost:9002,localhost:9003 --leader localhost:9001 2>&1 | jq -C" C-m

# Start node 2
tmux send-keys -t raft:2 "go run main.go --ip localhost --client-port 8002 --node-port 9002 --peers localhost:9001,localhost:9003 --leader localhost:9001 2>&1 | jq -C" C-m

# Start node 3
tmux send-keys -t raft:3 "go run main.go --ip localhost --client-port 8003 --node-port 9003 --peers localhost:9001,localhost:9002 --leader localhost:9001 2>&1 | jq -C" C-m

# Client (ready to be executed manually)
tmux send-keys -t raft:4 "go run client/client.go --node localhost:8001 --message 'hi'"

# Attach to the tmux session
tmux attach-session -t raft
