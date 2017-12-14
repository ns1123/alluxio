package cmd

import (
	"io"
	"path/filepath"
	"testing"
)

var erasedOutputs = map[string]string{
	"testdata/no_extension_java":                     "common code\nenterprise code\n",
	"testdata/no_extension_sh":                       "common code\nenterprise code\n",
	"testdata/no_extension_shebang":                  "#!bin/bash\nenterprise code\n",
	"testdata/revisioned_dir/revisioned_ok.xml":      "foo\nhello\nbar\n",
	"testdata/revisioned_ok.java":                    "foo\nhello\nbar\n",
	"testdata/revisioned_ok.jsp":                     "foo\nhello\nbar\n",
	"testdata/revisioned_ok.md":                      "foo\nhello\nbar\n",
	"testdata/revisioned_ok.properties":              "foo\nhello\nbar\n",
	"testdata/revisioned_ok.proto":                   "foo\nhello\nbar\n",
	"testdata/revisioned_ok.sh":                      "foo\nhello\nbar\n",
	"testdata/revisioned_ok.xml":                     "foo\nhello\nbar\n",
	"testdata/revisioned_ok_replace_empty_line.java": "foo\nhello\nbar\n",
	"testdata/revisioned_ok.yaml":                    "foo\nhello\nbar\n",
	"testdata/revisioned_ok.yaml.template":           "foo\nhello\nbar\n",
}

func TestErase(t *testing.T) {
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
		return eraseFile(path, writerFn)
	}
	if err := tree.walk("./testdata", emptyFn, walkFn); err != nil {
		t.Fatalf("%v", err)
	}
	for path, want := range erasedOutputs {
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
