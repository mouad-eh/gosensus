package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type PersistentState struct {
	CurrentTerm  int        `json:"current_term"`
	VotedFor     string     `json:"voted_for"`
	CommitLength int        `json:"commit_length"`
	Log          []LogEntry `json:"log"`
}

type LogEntry struct {
	Message string `json:"message"`
	Term    int    `json:"term"`
}

type JSONStorage struct {
	state    PersistentState
	filePath string
}

func NewJSONStorage(nodeID string) *JSONStorage {
	return &JSONStorage{
		state:    PersistentState{},
		filePath: filepath.Join("state", nodeID, "state.json"),
	}
}

func (s *JSONStorage) Init(nodeID string) error {
	if _, err := os.Stat(s.filePath); err == nil {
		return nil
	}

	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create state directory: %v", err)
	}

	return s.saveState()
}

func (s *JSONStorage) saveState() error {
	file, err := os.OpenFile(s.filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open state file: %v", err)
	}
	defer file.Close()

	json.NewEncoder(file).Encode(s.state)

	return nil
}

func (s *JSONStorage) LoadState() (currentTerm int, votedFor string, commitLength int, log []LogEntry, err error) {
	file, err := os.OpenFile(s.filePath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, "", 0, []LogEntry{}, fmt.Errorf("failed to open state file: %v", err)
	}
	defer file.Close()

	json.NewDecoder(file).Decode(&s.state)

	return s.state.CurrentTerm, s.state.VotedFor, s.state.CommitLength, s.state.Log, nil
}

func (s *JSONStorage) SaveCurrentTerm(currentTerm int) error {
	s.state.CurrentTerm = currentTerm
	return s.saveState()
}

func (s *JSONStorage) SaveVotedFor(votedFor string) error {
	s.state.VotedFor = votedFor
	return s.saveState()
}

func (s *JSONStorage) SaveCommitLength(commitLength int) error {
	s.state.CommitLength = commitLength
	return s.saveState()
}

func (s *JSONStorage) AppendLog(entry LogEntry) error {
	s.state.Log = append(s.state.Log, entry)
	return s.saveState()
}

func (s *JSONStorage) TrimLog(startIndex int) error {
	s.state.Log = s.state.Log[:startIndex]
	return s.saveState()
}
