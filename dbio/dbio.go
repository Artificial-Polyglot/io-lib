package dbio

import (
	"database/sql"
	"fmt"
	"io"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

// OpenDB reads a serialized sqlite database from stdin (if piped) or creates
// a fresh one, ensures the standard schema exists, and returns an open *sql.DB
// along with the temp file path. The caller must call OutputDB or CloseDB when done.
func OpenDB() (*sql.DB, string, error) {
	db, dbPath, err := readDB(os.Stdin, isTerminal(os.Stdin))
	if err != nil {
		return nil, "", err
	}
	if err = createSchema(db); err != nil {
		db.Close()
		os.Remove(dbPath)
		return nil, "", err
	}
	return db, dbPath, nil
}

// OutputDB writes the database to stdout if piped, or prints a message to
// stderr if stdout is a terminal. It closes the database and removes the
// temp file in all cases.
func OutputDB(db *sql.DB, dbPath string) error {
	defer os.Remove(dbPath)

	if isTerminal(os.Stdout) {
		fmt.Fprintln(os.Stderr, "stdout is a terminal, skipping database output")
		fmt.Fprintln(os.Stderr, "pipe to a file or another component to capture the database")
		db.Close()
		return nil
	}

	return writeDB(db, dbPath, os.Stdout)
}

// CloseDB closes the database and removes the temp file without writing output.
func CloseDB(db *sql.DB, dbPath string) {
	db.Close()
	os.Remove(dbPath)
}

func isTerminal(f *os.File) bool {
	stat, err := f.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}

func readDB(r io.Reader, terminal bool) (*sql.DB, string, error) {
	tmpFile, err := os.CreateTemp("", "arty-*.db")
	if err != nil {
		return nil, "", fmt.Errorf("creating temp db: %w", err)
	}
	tmpPath := tmpFile.Name()

	if !terminal {
		_, err = io.Copy(tmpFile, r)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return nil, "", fmt.Errorf("reading db from stdin: %w", err)
		}
	}
	tmpFile.Close()

	db, err := sql.Open("sqlite3", tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return nil, "", fmt.Errorf("opening db: %w", err)
	}

	if err = db.Ping(); err != nil {
		db.Close()
		os.Remove(tmpPath)
		return nil, "", fmt.Errorf("pinging db: %w", err)
	}

	return db, tmpPath, nil
}

func writeDB(db *sql.DB, dbPath string, w io.Writer) error {
	if err := db.Close(); err != nil {
		return fmt.Errorf("closing db before write: %w", err)
	}

	f, err := os.Open(dbPath)
	if err != nil {
		return fmt.Errorf("opening db file for output: %w", err)
	}
	defer f.Close()

	_, err = io.Copy(w, f)
	if err != nil {
		return fmt.Errorf("writing db to stdout: %w", err)
	}
	return nil
}

func createSchema(db *sql.DB) error {
	stmts := []string{
		`PRAGMA temp_store = MEMORY`,
		`CREATE TABLE IF NOT EXISTS ident (
			dataset_id INTEGER PRIMARY KEY AUTOINCREMENT,
			bible_id TEXT NOT NULL,
			audio_OT_id TEXT NOT NULL,
			audio_NT_id TEXT NOT NULL,
			text_OT_id TEXT NOT NULL,
			text_NT_id TEXT NOT NULL,
			text_source TEXT NOT NULL,
			language_iso TEXT NOT NULL,
			asr_language_iso TEXT NOT NULL DEFAULT '',
			version_code TEXT NOT NULL,
			language_id INTEGER NOT NULL,
			rolv_id INTEGER NOT NULL,
			alphabet TEXT NOT NULL,
			language_name TEXT NOT NULL,
			version_name TEXT NOT NULL) STRICT`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ident_bible_idx ON ident (bible_id)`,
		`CREATE TABLE IF NOT EXISTS scripts (
			script_id INTEGER PRIMARY KEY AUTOINCREMENT,
			dataset_id INTEGER NOT NULL,
			book_id TEXT NOT NULL,
			chapter_num INTEGER NOT NULL,
			chapter_end INTEGER NOT NULL,
			verse_str TEXT NOT NULL,
			verse_end TEXT NOT NULL,
			verse_num INTEGER NOT NULL,
			audio_file TEXT NOT NULL,
			script_num TEXT NOT NULL,
			usfm_style TEXT NOT NULL DEFAULT '',
			person TEXT NOT NULL DEFAULT '',
			actor TEXT NOT NULL DEFAULT '',
			script_text TEXT NOT NULL,
			uroman TEXT NOT NULL DEFAULT '',
			script_begin_ts REAL NOT NULL DEFAULT 0.0,
			script_end_ts REAL NOT NULL DEFAULT 0.0,
			fa_score REAL NOT NULL DEFAULT 0.0,
			FOREIGN KEY(dataset_id) REFERENCES ident(dataset_id)) STRICT`,
		`CREATE UNIQUE INDEX IF NOT EXISTS scripts_idx
			ON scripts (book_id, chapter_num, verse_str)`,
		`CREATE INDEX IF NOT EXISTS script_num_idx ON scripts (script_num)`,
		`CREATE INDEX IF NOT EXISTS scripts_file_idx ON scripts (audio_file)`,
		`CREATE TABLE IF NOT EXISTS words (
			word_id INTEGER PRIMARY KEY AUTOINCREMENT,
			script_id INTEGER NOT NULL,
			word_seq INTEGER NOT NULL,
			verse_num INTEGER NOT NULL,
			ttype TEXT NOT NULL DEFAULT 'W',
			word TEXT NOT NULL,
			uroman TEXT NOT NULL DEFAULT '',
			word_begin_ts REAL NOT NULL DEFAULT 0.0,
			word_end_ts REAL NOT NULL DEFAULT 0.0,
			fa_score REAL NOT NULL DEFAULT 0.0,
			word_enc TEXT NOT NULL DEFAULT '',
			src_word_enc TEXT NOT NULL DEFAULT '',
			word_multi_enc TEXT NOT NULL DEFAULT '',
			src_word_multi_enc TEXT NOT NULL DEFAULT '',
			FOREIGN KEY(script_id) REFERENCES scripts(script_id)) STRICT`,
		`CREATE UNIQUE INDEX IF NOT EXISTS words_idx
			ON words (script_id, word_seq)`,
		`CREATE TABLE IF NOT EXISTS log (
			log_id INTEGER PRIMARY KEY AUTOINCREMENT,
			component TEXT NOT NULL,
			level TEXT NOT NULL,
			message TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT (datetime('now'))) STRICT`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("schema exec %q: %w", stmt[:min(len(stmt), 60)], err)
		}
	}
	return nil
}
