package cmd

import (
	"testing"
)

type commentTypeTestCase struct {
	filename string
	ct       commentType
}

func TestInferCommentType(t *testing.T) {
	testCases := []commentTypeTestCase{
		{
			filename: "./testdata/no_extension_sh",
			ct:       poundType,
		},
		{
			filename: "./testdata/revisioned_ok.java",
			ct:       slashType,
		},
		{
			filename: "./testdata/revisioned_ok.md",
			ct:       markupType,
		},
		{
			filename: "./testdata/revisioned_ok.properties",
			ct:       poundType,
		},
		{
			filename: "./testdata/revisioned_ok.sh",
			ct:       poundType,
		},
		{
			filename: "./testdata/revisioned_ok.yaml",
			ct:       poundType,
		},
		{
			filename: "./testdata/revisioned_ok.xml",
			ct:       markupType,
		},
		{
			filename: "./testdata/revisioned_ok.xml.template",
			ct:       markupType,
		},
		{
			filename: "./testdata/revisioned_ok.sh.template",
			ct:       poundType,
		},
		{
			filename: "./testdata/revisioned_ok.properties.template",
			ct:       poundType,
		},
		{
			filename: "./testdata/revisioned_ok.yaml.template",
			ct:       poundType,
		},
	}

	for _, tc := range testCases {
		ct, err := inferCommentType(tc.filename)
		if err != nil {
			t.Errorf("Failed to infer file type: %v", err)
		}
		if got, want := ct, tc.ct; got != want {
			t.Errorf("unexpected file type for %v: got %v, want %v", tc.filename, got, want)
		}
	}
}
