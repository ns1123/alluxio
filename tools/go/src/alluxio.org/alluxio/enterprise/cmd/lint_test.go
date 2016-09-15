package cmd

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var expectedWarnings = generateWarnings()

func generateWarnings() map[string][]warning{
	result := map[string][]warning{}
	for _, path := range revisionedPaths {
		if strings.Index(path, "revisioned_fail") != -1 {
			name := filepath.Base(path)
			result[filepath.Join("testdata", path)] = []warning{
				warning{
					Filename: name,
					Line:     1,
					Message:  `annotation "ENTERPRISE END" is not preceeded by either "ENTERPRISE ADD" or "ENTERPRISE REMOVE"`,
				},
				warning{
					Filename: name,
					Line:     3,
					Message:  `annotation "ENTERPRISE ADD" is not followed by annotation "ENTERPRISE END"`,
				},
				warning{
					Filename: name,
					Line:     5,
					Message:  `annotation "ENTERPRISE REPLACE" is not followed by annotation "ENTERPRISE WITH"`,
				},
				warning{
					Filename: name,
					Line:     9,
					Message:  `annotation "ENTERPRISE REMOVE" is not followed by annotation "ENTERPRISE END"`,
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
			t.Errorf("unexpected warnings:\ngot:\n%s\nwant:\n%s\n", gotData, wantData)
		}
	}
}
