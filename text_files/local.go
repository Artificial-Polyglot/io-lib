package text_files

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func ReadDir(dir string, suffix string) ([]string, error) {
	var results []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading directory %s: %w", dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(strings.ToLower(entry.Name()), strings.ToLower(suffix)) {
			results = append(results, filepath.Join(dir, entry.Name()))
		}
	}
	return results, nil
}
