package cmd

import (
	"io"
	"path/filepath"
	"testing"
)

var expectedOutputs = map[string]string{
	"testdata/revisioned_dir/revisioned_ok.xml":      "hello\nworld\n",
	"testdata/revisioned_ok.java":                    "hello\nworld\n",
	"testdata/revisioned_ok.properties":              "hello\nworld\n",
	"testdata/revisioned_ok.sh":                      "hello\nworld\n",
	"testdata/revisioned_ok.xml":                     "hello\nworld\n",
	"testdata/revisioned_ok_replace_empty_line.java": "hello\n\n",
}

type buffer string

func (b *buffer) Close() error {
	return nil
}

func (b *buffer) Write(p []byte) (int, error) {
	*b += buffer(p)
	return len(p), nil
}

func (b *buffer) String() string {
	return string(*b)
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
	for path, want := range expectedOutputs {
		if got := writers[path].String(); got != want {
			t.Errorf("unexpected output:\ngot\n%#v\nwant\n%#v\n", got, want)
		}
	}
}
