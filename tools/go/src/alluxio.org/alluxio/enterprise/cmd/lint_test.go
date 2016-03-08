package cmd

import (
	"bytes"
	"testing"
)

var revisionedPaths = []string{
	"excluded_fail.xml",
	"excluded_ok.xml",
	"revisioned_dir/excluded_fail.xml",
	"revisioned_dir/excluded_ok.xml",
	"revisioned_dir/revisioned_fail.xml",
	"revisioned_dir/revisioned_ok.xml",
	"revisioned_fail.java",
	"revisioned_fail.properties",
	"revisioned_fail.xml",
	"revisioned_ok.java",
	"revisioned_ok.properties",
	"revisioned_ok.xml",
}

const expectedOutput = `warnings for testdata/revisioned_dir/revisioned_fail.xml:
  line 7: annotation "ENTERPRISE END" is not preceeded by either "ENTERPRISE ADD" or "ENTERPRISE REPLACES"
warnings for testdata/revisioned_fail.java:
  line 1: annotation "ENTERPRISE END" is not preceeded by either "ENTERPRISE ADD" or "ENTERPRISE REPLACES"
  line 3: annotation "ENTERPRISE REPLACES" is not preceeded by "ENTERPRISE EDIT"
warnings for testdata/revisioned_fail.properties:
  line 1: annotation "ENTERPRISE ADD" is not followed by annotation "ENTERPRISE END"
warnings for testdata/revisioned_fail.xml:
  line 7: annotation "ENTERPRISE END" is not preceeded by either "ENTERPRISE ADD" or "ENTERPRISE REPLACES"
`

func TestLint(t *testing.T) {
	tree := &tree{}
	for _, revisionedPath := range revisionedPaths {
		tree.insert(revisionedPath)
	}
	buffer := bytes.Buffer{}
	if err := treeWalk("./testdata", tree, &buffer); err != nil {
		t.Fatalf("%v", err)
	}
	if buffer.String() != expectedOutput {
		t.Fatalf("unexpected output:\ngot\n%v\nwant\n%v\n", buffer.String(), expectedOutput)
	}
}
