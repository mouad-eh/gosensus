package raft

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewSugaredZapLogger(nodeID string) *zap.SugaredLogger {
	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.DebugLevel),
		Development: true,
		Encoding:    "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:      "timestamp",
			LevelKey:     "level",
			CallerKey:    "caller",
			MessageKey:   "msg",
			EncodeLevel:  zapcore.LowercaseLevelEncoder,
			EncodeTime:   zapcore.RFC3339TimeEncoder,
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
		OutputPaths: []string{"stdout"},
	}

	logger, _ := config.Build()
	logger = logger.With(zap.String("nodeID", nodeID))

	return logger.Sugar()
}
