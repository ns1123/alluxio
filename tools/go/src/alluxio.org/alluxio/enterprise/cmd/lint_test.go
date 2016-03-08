package cmd

import (
	"reflect"
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

var expectedWarnings = map[string][]warning{
	"testdata/revisioned_dir/revisioned_fail.xml": []warning{
		warning{
			filename: "revisioned_fail.xml",
			line:     7,
			message:  `annotation "ENTERPRISE END" is not preceeded by either "ENTERPRISE ADD" or "ENTERPRISE REPLACES"`,
		},
	},
	"testdata/revisioned_fail.java": []warning{
		warning{
			filename: "revisioned_fail.java",
			line:     1,
			message:  `annotation "ENTERPRISE END" is not preceeded by either "ENTERPRISE ADD" or "ENTERPRISE REPLACES"`,
		},
		warning{
			filename: "revisioned_fail.java",
			line:     3,
			message:  `annotation "ENTERPRISE REPLACES" is not preceeded by "ENTERPRISE EDIT"`,
		},
	},
	"testdata/revisioned_fail.properties": []warning{
		warning{
			filename: "revisioned_fail.properties",
			line:     1,
			message:  `annotation "ENTERPRISE ADD" is not followed by annotation "ENTERPRISE END"`,
		},
		warning{
			filename: "revisioned_fail.properties",
			line:     5,
			message:  `annotation "ENTERPRISE REPLACES" is not followed by annotation "ENTERPRISE END"`,
		},
	},
	"testdata/revisioned_fail.xml": []warning{
		warning{
			filename: "revisioned_fail.xml",
			line:     7,
			message:  `annotation "ENTERPRISE END" is not preceeded by either "ENTERPRISE ADD" or "ENTERPRISE REPLACES"`,
		},
	},
}

func TestLint(t *testing.T) {
	tree := &tree{}
	for _, revisionedPath := range revisionedPaths {
		tree.insert(revisionedPath)
	}
	warnings := map[string][]warning{}
	if err := treeWalk("./testdata", tree, warnings); err != nil {
		t.Fatalf("%v", err)
	}
	if !reflect.DeepEqual(expectedWarnings, warnings) {
		t.Errorf("unexpected warnings:\ngot\n%#v\nwant\n%#v\n", warnings, expectedWarnings)
	}
}
