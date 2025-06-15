package raft

import (
	"crypto/sha256"
	"encoding/hex"
)

// generateNodeID creates a consistent node ID from IP address and port
func generateNodeID(addr string) string {
	// Create a hash of the address
	hash := sha256.Sum256([]byte(addr))
	// Take first 8 characters of the hex representation for a shorter ID
	return hex.EncodeToString(hash[:])[:8]
}
