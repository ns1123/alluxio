package cmd

import (
	"fmt"
	"v.io/x/lib/cmdline"
)

const (
	flagRepoName    = "repo"
	flagWarningName = "fail-on-warning"
)

var (
	Root = &cmdline.Command{
		Name:  "enterprise",
		Short: "tool for manipuling enterprise version of the Alluxio open source",
		Long: `
The enteprise tool provides functionality for manipulating enterprise version fo the Alluxio
open source. For instance, the tool can be used for linting the enteprise source code annotations
or for reverting enterprise-only changes.
`,
		Children: []*cmdline.Command{
			cmdLint,
			cmdRevert,
		},
	}

	flagRepo    string
	flagWarning bool
)

func init() {
	Root.Flags.StringVar(&flagRepo, flagRepoName, "", "local path to the enterprise repository")
	cmdLint.Flags.BoolVar(&flagWarning, flagWarningName, false, "whether to return non-zero status when warnings are encountered")
}

func checkRootFlags() error {
	if flagRepo == "" {
		return fmt.Errorf("flag %v is required", flagRepoName)
	}
	return nil
}
