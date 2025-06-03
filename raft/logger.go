package raft

import (
	"fmt"
	"log"
	"os"
)

type Logger struct {
	infoLogger  *log.Logger
	errorLogger *log.Logger
}

func NewLogger(nodeID string) *Logger {
	infoLogger := log.New(os.Stdout, "", log.Ltime)
	errorLogger := log.New(os.Stderr, "", log.Ltime)
	infoLogger.SetPrefix(fmt.Sprintf("[INFO] %s ", nodeID))
	errorLogger.SetPrefix(fmt.Sprintf("[ERROR] %s ", nodeID))
	return &Logger{
		infoLogger:  infoLogger,
		errorLogger: errorLogger,
	}
}

func (l *Logger) Info(format string, v ...interface{}) {
	l.infoLogger.Printf(format, v...)
}

func (l *Logger) Error(format string, v ...interface{}) {
	l.errorLogger.Printf(format, v...)
}
