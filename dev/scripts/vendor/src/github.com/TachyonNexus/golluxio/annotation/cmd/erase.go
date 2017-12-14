package cmd

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"os"

	"v.io/x/lib/cmdline"
)

var cmdErase = &cmdline.Command{
	Name:   "erase",
	Short:  "Erases closed source annotations",
	Long:   "Erases closed source annotations.",
	Runner: cmdline.RunnerFunc(runErase),
}

func eraseFile(filename string, writerFn func(string) (io.WriteCloser, error)) error {
	ct, err := inferCommentType(filename)
	if err != nil {
		return err
	}
	if ct == unknownType {
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
		if err := sm.processErase(scanner.Text(), writer, ct); err != nil {
			return err
		}
		line++
		sm.next(scanner.Text(), line, ct)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func runErase(_ *cmdline.Env, _ []string) error {
	if err := checkRootFlags(); err != nil {
		return err
	}
	return Erase(flagRepo)
}

func Erase(repo string) error {
	if err := os.Chdir(repo); err != nil {
		return err
	}
	tree, err := newTree(repo)
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
		return eraseFile(path, writerFn)
	}
	excludedFn := func(path string) error {
		// No processing required for excluded files - we keep them when erasing annotations.
		return nil
	}
	if err := tree.walk(repo, excludedFn, walkFn); err != nil {
		return err
	}
	return nil
}
