package raft

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
	pb "github.com/mouad-eh/gosensus/rpc"
)

// Storage defines the interface for persistent storage operations
type Storage interface {
	// Initialize the storage
	Init(nodeID string) error
	// Close the storage
	Close() error
	// Save the current state
	SaveState(currentTerm int32, votedFor string, commitLength int32, log []pb.LogEntry) error
	// Load the current state
	LoadState() (currentTerm int32, votedFor string, commitLength int32, log []pb.LogEntry, err error)
}

// SQLiteStorage implements the Storage interface using SQLite
type SQLiteStorage struct {
	db *sql.DB
}

// NewSQLiteStorage creates a new SQLite storage instance
func NewSQLiteStorage() *SQLiteStorage {
	return &SQLiteStorage{}
}

func (s *SQLiteStorage) Init(nodeID string) error {
	// Create data directory if it doesn't exist
	stateDir := filepath.Join("state", nodeID)
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %v", err)
	}

	// Open SQLite database
	dbPath := filepath.Join(stateDir, "raft.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	s.db = db

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS state (
			key TEXT PRIMARY KEY,
			value TEXT
		);
		CREATE TABLE IF NOT EXISTS log_entries (
			entry_index INTEGER PRIMARY KEY,
			term INTEGER,
			message TEXT
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create tables: %v", err)
	}

	return nil
}

func (s *SQLiteStorage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SQLiteStorage) SaveState(currentTerm int32, votedFor string, commitLength int32, log []pb.LogEntry) error {
	// Save current term
	_, err := s.db.Exec("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
		"current_term", currentTerm)
	if err != nil {
		return fmt.Errorf("failed to save current term: %v", err)
	}

	// Save voted for
	_, err = s.db.Exec("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
		"voted_for", votedFor)
	if err != nil {
		return fmt.Errorf("failed to save voted for: %v", err)
	}

	// Save commit length
	_, err = s.db.Exec("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
		"commit_length", commitLength)
	if err != nil {
		return fmt.Errorf("failed to save commit length: %v", err)
	}

	// Save log entries
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Clear existing log entries
	_, err = tx.Exec("DELETE FROM log_entries")
	if err != nil {
		return fmt.Errorf("failed to clear log entries: %v", err)
	}

	// Insert new log entries
	stmt, err := tx.Prepare("INSERT INTO log_entries (entry_index, term, message) VALUES (?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare log insert statement: %v", err)
	}
	defer stmt.Close()

	for i, entry := range log {
		_, err = stmt.Exec(i, entry.Term, entry.Message)
		if err != nil {
			return fmt.Errorf("failed to insert log entry: %v", err)
		}
	}

	return tx.Commit()
}

func (s *SQLiteStorage) LoadState() (currentTerm int32, votedFor string, commitLength int32, log []pb.LogEntry, err error) {
	// Load current term
	var currentTermStr string
	err = s.db.QueryRow("SELECT value FROM state WHERE key = ?", "current_term").Scan(&currentTermStr)
	if err != nil && err != sql.ErrNoRows {
		return 0, "", 0, nil, fmt.Errorf("failed to load current term: %v", err)
	}
	if currentTermStr != "" {
		term, err := strconv.ParseInt(currentTermStr, 10, 32)
		if err != nil {
			return 0, "", 0, nil, fmt.Errorf("failed to parse current term: %v", err)
		}
		currentTerm = int32(term)
	}

	// Load voted for
	err = s.db.QueryRow("SELECT value FROM state WHERE key = ?", "voted_for").Scan(&votedFor)
	if err != nil && err != sql.ErrNoRows {
		return 0, "", 0, nil, fmt.Errorf("failed to load voted for: %v", err)
	}

	// Load commit length
	var commitLengthStr string
	err = s.db.QueryRow("SELECT value FROM state WHERE key = ?", "commit_length").Scan(&commitLengthStr)
	if err != nil && err != sql.ErrNoRows {
		return 0, "", 0, nil, fmt.Errorf("failed to load commit length: %v", err)
	}
	if commitLengthStr != "" {
		length, err := strconv.ParseInt(commitLengthStr, 10, 32)
		if err != nil {
			return 0, "", 0, nil, fmt.Errorf("failed to parse commit length: %v", err)
		}
		commitLength = int32(length)
	}

	// Load log entries
	rows, err := s.db.Query("SELECT term, message FROM log_entries ORDER BY entry_index")
	if err != nil {
		return 0, "", 0, nil, fmt.Errorf("failed to query log entries: %v", err)
	}
	defer rows.Close()

	log = []pb.LogEntry{}
	for rows.Next() {
		var term int32
		var message string
		if err := rows.Scan(&term, &message); err != nil {
			return 0, "", 0, nil, fmt.Errorf("failed to scan log entry: %v", err)
		}
		log = append(log, pb.LogEntry{
			Term:    term,
			Message: message,
		})
	}

	if err = rows.Err(); err != nil {
		return 0, "", 0, nil, fmt.Errorf("error iterating log entries: %v", err)
	}

	return currentTerm, votedFor, commitLength, log, nil
}
