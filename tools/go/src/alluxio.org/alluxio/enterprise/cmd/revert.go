package cmd

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"os"

	"io"
	"v.io/x/lib/cmdline"
)

var cmdRevert = &cmdline.Command{
	Name:   "revert",
	Short:  "Reverts enterprise-only changes of the open source code base",
	Long:   "This command reverts enterprise-only changes of the open source code base.",
	Runner: cmdline.RunnerFunc(runRevert),
}

func revert(filename string, writerFn func(string) (io.WriteCloser, error)) error {
	ft := inferFileType(filename)
	if ft == unknownType {
		// skip unknown types
		return nil
	}
	input, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	writer, err := writerFn(filename)
	if err != nil {
		return err
	}
	defer writer.Close()
	sm, line := newStateMachine(filename), 0
	scanner := bufio.NewScanner(bytes.NewReader(input))
	for scanner.Scan() {
		if err := sm.process(scanner.Text(), writer, ft); err != nil {
			return err
		}
		line++
		sm.next(scanner.Text(), line, ft)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func runRevert(env *cmdline.Env, args []string) error {
	if err := checkRootFlags(); err != nil {
		return err
	}
	if err := os.Chdir(flagRepo); err != nil {
		return err
	}
	tree, err := newTree(flagRepo)
	if err != nil {
		return err
	}
	writerFn := func(filename string) (io.WriteCloser, error) {
		fileInfo, err := os.Stat(filename)
		if err != nil {
			return nil, err
		}
		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileInfo.Mode())
		if err != nil {
			return nil, err
		}
		return f, nil
	}
	walkFn := func(path string) error {
		return revert(path, writerFn)
	}
	excludedFn := func(path string) error {
		return os.RemoveAll(path)
	}
	if err := tree.walk(flagRepo, excludedFn, walkFn); err != nil {
		return err
	}
	return nil
}
