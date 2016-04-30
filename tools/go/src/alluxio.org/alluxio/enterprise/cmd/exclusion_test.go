package cmd

import (
	"testing"
)

type exclusionTestCase struct {
	path string
	want bool
}

func TestExclusion(t *testing.T) {
	exclusions, err := readExclusions("./testdata")
	if err != nil {
		t.Fatalf("%v", err)
	}
	testCases := []exclusionTestCase{
		exclusionTestCase{
			path: "excluded_dir",
			want: true,
		},
		exclusionTestCase{
			path: "excluded_dir2",
			want: true,
		},
		exclusionTestCase{
			path: "excluded_fail.xml",
			want: true,
		},
		exclusionTestCase{
			path: "excluded_ok.xml",
			want: true,
		},
		exclusionTestCase{
			path: "revisioned_ok.xml",
			want: false,
		},
	}
	for _, testCase := range testCases {
		want := testCase.want
		if _, got := exclusions[testCase.path]; got != want {
			t.Fatal("unexpected result for %v: got %v, want %v", testCase.path, got, want)
		}
	}
}
