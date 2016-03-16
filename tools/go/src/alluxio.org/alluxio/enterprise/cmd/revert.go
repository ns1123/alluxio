package cmd

import (
	"v.io/x/lib/cmdline"
)

var cmdRevert = &cmdline.Command{
	Name:   "revert",
	Short:  "Reverts enterprise-only changes of the open source code base",
	Long:   "This command reverts enterprise-only changes of the open source code base.",
	Runner: cmdline.RunnerFunc(runRevert),
}

func runRevert(env *cmdline.Env, args []string) error {
	if err := checkRootFlags(); err != nil {
		return err
	}
	// TODO
	return nil
}
