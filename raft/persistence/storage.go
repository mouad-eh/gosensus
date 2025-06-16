package persistence

type Storage interface {
	Init(nodeID string) error
	SaveCurrentTerm(currentTerm int) error
	SaveVotedFor(votedFor string) error
	SaveCommitLength(commitLength int) error
	AppendLog(entry LogEntry) error
	TrimLog(lastIndex int) error
	LoadState() (currentTerm int, votedFor string, commitLength int, log []LogEntry, err error)
}
