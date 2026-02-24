package dbio

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
)

// Logger writes log entries to both a database table and stderr.
// INFO and WARN go to the database only. ERROR goes to both.
type Logger struct {
	db        *sql.DB
	component string
}

// NewLogger creates a logger that tags entries with the given component name.
func NewLogger(db *sql.DB, component string) *Logger {
	return &Logger{db: db, component: component}
}

// Info logs an informational message to the database.
func (l *Logger) Info(args ...any) {
	msg := joinArgs(args)
	l.insertLog("INFO", msg)
}

// Warn logs a warning message to the database.
func (l *Logger) Warn(args ...any) {
	msg := joinArgs(args)
	l.insertLog("WARN", msg)
}

// Error logs an error message to the database and stderr.
func (l *Logger) Error(args ...any) {
	msg := joinArgs(args)
	l.insertLog("ERROR", msg)
	fmt.Fprintf(os.Stderr, "ERROR [%s]: %s\n", l.component, msg)
}

// Fatal logs an error message to the database and stderr, then exits.
func (l *Logger) Fatal(args ...any) {
	l.Error(args...)
	os.Exit(1)
}

func (l *Logger) insertLog(level string, message string) {
	if l.db == nil {
		return
	}
	_, err := l.db.Exec(
		`INSERT INTO log (component, level, message) VALUES (?, ?, ?)`,
		l.component, level, message,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN [%s]: failed to write log to db: %v\n", l.component, err)
	}
}

func joinArgs(args []any) string {
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = fmt.Sprint(a)
	}
	return strings.Join(parts, " ")
}
