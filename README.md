# Gosensus

A Go package that implements distributed consensus algorithms. Currently supports Raft consensus protocol with plans to implement Paxos in the future.


## Project Structure

```
gosensus/
├── main.go              # Main application entry point
├── start.sh             # Script to run a 3-node Raft cluster
├── go.mod               # Go module dependencies
├── raft/                # Raft consensus implementation
│   ├── raft.go          # Core Raft algorithm
│   ├── grpc_server.go   # gRPC server implementation
│   ├── raft.proto       # Protocol buffer definitions
│   ├── logger.go        # Logging utilities
│   ├── transport.go     # Network transport layer
│   ├── utils.go         # Utility functions
│   ├── persistence/     # State persistence layer
│   └── rpc/             # Generated gRPC code
├── clients/             # Client applications
│   └── raft.go          # Raft client implementation
└── state/               # Persistent state storage
```

## Prerequisites

- Go 1.23.4 or later
- Protocol Buffers compiler (`protoc`)
- tmux (for running the demo cluster)
- jq (for JSON formatting in logs)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/mouad-eh/gosensus.git
cd gosensus
```

2. Install dependencies:
```bash
go mod download
```

3. Generate Protocol Buffer code (if you modify `raft/raft.proto`):
```bash
cd raft && protoc --go_out=. --go-grpc_out=. raft.proto
```

## Quick Start

### Running a Raft Cluster

The easiest way to run a 3-node Raft cluster is using the provided script:

```bash
./start.sh
```

This script will:
- Clean up any existing state
- Start 3 Raft nodes on different ports
- Open a tmux session with separate windows for each node
- Provide a client window ready for testing

### Manual Node Setup

To run individual nodes manually:

```bash
go run main.go --ip 127.0.0.1 --client-port 8001 --node-port 9001 --peers 127.0.0.1:9002,127.0.0.1:9003
```

Parameters:
- `--ip`: Node IP address
- `--client-port`: Port for client connections
- `--node-port`: Port for inter-node communication
- `--peers`: Comma-separated list of peer addresses

### Using the Client

To send messages to the cluster:

```bash
go run clients/raft.go --node localhost:8001 --message "Hello, Raft!"
```

## State Management

The cluster state is persisted in the `state/` directory. Each node maintains its own state files:

- **Log entries**: Replicated log of all commands
- **Term information**: Current term and voted-for information
- **Commit index**: Last committed log entry

To inspect the state, check the `state/` folder after running the cluster.

## Acknowledgments

This implementation is based on the Raft consensus algorithm as described in [Distributed Systems lecture series](https://www.youtube.com/playlist?list=PLeKd45zvjcDFUEv_ohr_HdUFe97RItdiB) by Martin Kleppmann. 