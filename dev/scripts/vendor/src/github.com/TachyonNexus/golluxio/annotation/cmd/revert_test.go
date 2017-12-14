package cmd

import (
	"io"
	"path/filepath"
	"testing"
)

var revertedOutputs = map[string]string{
	"testdata/no_extension_java":                     "common code\n",
	"testdata/no_extension_sh":                       "common code\n",
	"testdata/no_extension_shebang":                  "#!bin/bash\n",
	"testdata/revisioned_dir/revisioned_ok.xml":      "hello\nworld\npeace\n",
	"testdata/revisioned_ok.java":                    "hello\nworld\npeace\n",
	"testdata/revisioned_ok.jsp":                     "hello\nworld\npeace\n",
	"testdata/revisioned_ok.md":                      "hello\nworld\npeace\n",
	"testdata/revisioned_ok.properties":              "hello\nworld\npeace\n",
	"testdata/revisioned_ok.proto":                   "hello\nworld\npeace\n",
	"testdata/revisioned_ok.sh":                      "hello\nworld\npeace\n",
	"testdata/revisioned_ok.xml":                     "hello\nworld\npeace\n",
	"testdata/revisioned_ok_replace_empty_line.java": "hello\n\npeace\n",
	"testdata/revisioned_ok.yaml":                    "hello\nworld\npeace\n",
	"testdata/revisioned_ok.yaml.template":           "hello\nworld\npeace\n",
}

func TestRevert(t *testing.T) {
	tree := &tree{}
	for _, revisionedPath := range revisionedPaths {
		tree.insert(revisionedPath)
	}
	writers := map[string]*buffer{}
	for _, path := range revisionedPaths {
		b := buffer("")
		writers[filepath.Join("testdata", path)] = &b
	}
	writerFn := func(filename string) (io.WriteCloser, error) {
		return writers[filename], nil
	}
	walkFn := func(path string) error {
		return revert(path, writerFn)
	}
	if err := tree.walk("./testdata", emptyFn, walkFn); err != nil {
		t.Fatalf("%v", err)
	}
	for path, want := range revertedOutputs {
		result, ok := writers[path]
		if !ok {
			t.Errorf("no result found for %v, make sure it's in the revisionedPaths list", path)
			return
		}
		if got := result.String(); got != want {
			t.Errorf("unexpected output for file %v:\ngot\n%#v\nwant\n%#v\n", path, got, want)
		}
	}
}
