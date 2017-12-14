package cmd

import (
	"fmt"

	"v.io/x/lib/cmdline"
)

const (
	flagRepoName          = "repo"
	flagFailOnWarningName = "fail-on-warning"
)

var (
	Root = &cmdline.Command{
		Name:  "annotation",
		Short: "tool for manipuling closed source annotations of the Alluxio open source",
		Long: `
The annotation tool provides functionality for manipulating closed source annotations
of the Alluxio open source. For instance, the tool can be used for linting the closed
source code annotations or for reverting closed source changes.
`,
		Children: []*cmdline.Command{
			cmdErase,
			cmdLint,
			cmdRevert,
		},
	}

	flagRepo          string
	flagFailOnWarning bool
)

func init() {
	Root.Flags.StringVar(&flagRepo, flagRepoName, "", "local path to the closed source repository")
	cmdLint.Flags.BoolVar(&flagFailOnWarning, flagFailOnWarningName, false, "whether to return non-zero status when warnings are encountered")
}

func checkRootFlags() error {
	if flagRepo == "" {
		return fmt.Errorf("flag %v is required", flagRepoName)
	}
	return nil
}
