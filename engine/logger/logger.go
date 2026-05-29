package logger

import (
	"log/slog"
	"os"
)

type Mode int

const (
	Development Mode = iota
	Production
)

type Logger struct {
	*slog.Logger
}

func New(mode Mode) *Logger {
	var handler slog.Handler
	switch mode {
	case Production:
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})
	default:
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	}
	return &Logger{Logger: slog.New(handler)}
}

func NewWithHandler(h slog.Handler) *Logger {
	return &Logger{Logger: slog.New(h)}
}

func Default() *Logger {
	return New(Development)
}
