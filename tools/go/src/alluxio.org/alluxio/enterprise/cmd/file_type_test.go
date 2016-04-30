package cmd

import (
	"testing"
)

type fileTypeTestCase struct {
	filename string
	ft       fileType
}

func TestInferFileType(t *testing.T) {
	testCases := []fileTypeTestCase{
		fileTypeTestCase{
			filename: "./testdata/revisioned_ok.java",
			ft:       javaType,
		},
		fileTypeTestCase{
			filename: "./testdata/revisioned_ok.properties",
			ft:       propertiesType,
		},
		fileTypeTestCase{
			filename: "./testdata/revisioned_ok.sh",
			ft:       shellType,
		},
		fileTypeTestCase{
			filename: "./testdata/revisioned_ok.xml",
			ft:       xmlType,
		},
		fileTypeTestCase{
			filename: "./testdata/revisioned_ok.xml.template",
			ft:       xmlType,
		},
		fileTypeTestCase{
			filename: "./testdata/revisioned_ok.sh.template",
			ft:       shellType,
		},
		fileTypeTestCase{
			filename: "./testdata/revisioned_ok.properties.template",
			ft:       propertiesType,
		},
	}

	for _, tc := range testCases {
		if got, want := inferFileType(tc.filename), tc.ft; got != want {
			t.Errorf("unexpected file type: got %v, want %v", got, want)
		}
	}
}
