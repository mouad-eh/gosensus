package raft

import "fmt"


func (s *Server) setCurrentTerm(term int32) error {
	s.CurrentTerm = term
	if err := s.storage.SaveCurrentTerm(term); err != nil {
		return fmt.Errorf("failed to save state after setting term: %w", err)
	}
	return nil
}

func (s *Server) setVotedFor(votedFor string) error {
	s.VotedFor = votedFor
	if err := s.storage.SaveVotedFor(votedFor); err != nil {
		return fmt.Errorf("failed to save state after setting votedFor: %w", err)
	}
	return nil
}

func (s *Server) appendLog(entry LogEntry) error {
	s.Log = append(s.Log, entry)
	if err := s.storage.AppendLog(entry); err != nil {
		return fmt.Errorf("failed to save state after appending log: %w", err)
	}
	return nil
}

func (s *Server) trimLog(startIndex int32) error {
	s.Log = s.Log[:startIndex]
	if err := s.storage.TrimLog(startIndex); err != nil {
		return fmt.Errorf("failed to save state after trimming log: %w", err)
	}
	return nil
}

func (s *Server) setCommitLength(length int32) error {
	s.CommitLength = length
	if err := s.storage.SaveCommitLength(length); err != nil {
		return fmt.Errorf("failed to save state after setting commit length: %w", err)
	}
	return nil
}