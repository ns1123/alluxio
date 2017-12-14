package cmd

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var expectedWarnings = generateWarnings()

func generateWarnings() map[string][]warning {
	result := map[string][]warning{}
	for _, path := range revisionedPaths {
		if strings.Index(path, "revisioned_fail") != -1 {
			name := filepath.Base(path)
			result[filepath.Join("testdata", path)] = []warning{
				{
					Filename: name,
					Line:     1,
					Message:  `annotation "ALLUXIO CS END" is not preceeded by either "ALLUXIO CS ADD" or "ALLUXIO CS REMOVE"`,
				},
				{
					Filename: name,
					Line:     3,
					Message:  `annotation "ALLUXIO CS ADD" is not followed by annotation "ALLUXIO CS END"`,
				},
				{
					Filename: name,
					Line:     5,
					Message:  `annotation "ALLUXIO CS REPLACE" is not followed by annotation "ALLUXIO CS WITH"`,
				},
				{
					Filename: name,
					Line:     9,
					Message:  `annotation "ALLUXIO CS REMOVE" is not followed by annotation "ALLUXIO CS END"`,
				},
			}
		}
	}
	return result
}

func TestLint(t *testing.T) {
	tree := &tree{}
	for _, revisionedPath := range revisionedPaths {
		tree.insert(revisionedPath)
	}
	warnings := map[string][]warning{}
	walkFn := func(path string) error {
		return lint(path, warnings)
	}
	if err := tree.walk("./testdata", emptyFn, walkFn); err != nil {
		t.Fatalf("%v", err)
	}
	for _, path := range revisionedPaths {
		path = filepath.Join("testdata", path)
		if got, want := warnings[path], expectedWarnings[path]; !reflect.DeepEqual(got, want) {
			gotData, err := json.MarshalIndent(got, "", "  ")
			if err != nil {
				t.Fatalf("%v", err)
			}
			wantData, err := json.MarshalIndent(want, "", "  ")
			if err != nil {
				t.Fatalf("%v", err)
			}
			t.Errorf("unexpected warnings for %v:\ngot:\n%s\nwant:\n%s\n", path, gotData, wantData)
		}
	}
}
