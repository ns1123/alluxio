package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"v.io/x/lib/cmdline"
)

var cmdLint = &cmdline.Command{
	Name:   "lint",
	Short:  "Checks validity of enterprise annotations",
	Long:   "This command checks validity of enterprise annotations.",
	Runner: cmdline.RunnerFunc(runLint),
}

func lint(filename string, w io.Writer) error {
	ft := inferFileType(filename)
	if ft == unknownType {
		// skip unknown types
		return nil
	}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	sm, line, warnings := newStateMachine(filename), 0, []string{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line++
		warnings = append(warnings, sm.next(strings.TrimSpace(scanner.Text()), line, ft)...)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(warnings) != 0 {
		fmt.Fprintf(w, "warnings for %v:\n", filename)
		for _, warning := range warnings {
			fmt.Fprintf(w, "  %v\n", warning)
		}
	}
	return nil
}

func treeWalk(dirname string, revisionedObjects *tree, w io.Writer) error {
	exclusions, err := readExclusions(dirname)
	if err != nil {
		return err
	}
	infos, err := ioutil.ReadDir(dirname)
	if err != nil {
		return err
	}
	for _, info := range infos {
		if _, ok := exclusions[info.Name()]; ok {
			continue // skip excluded files
		}
		if !revisionedObjects.contains(info.Name()) {
			continue // skip unrevisioned objects
		}
		if info.IsDir() {
			if err := treeWalk(filepath.Join(dirname, info.Name()), revisionedObjects.get(info.Name()), w); err != nil {
				return err
			}
		} else {
			if err := lint(filepath.Join(dirname, info.Name()), w); err != nil {
				return err
			}
		}
	}
	return nil
}

func runLint(env *cmdline.Env, args []string) error {
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
	return treeWalk(flagRepo, tree, os.Stdout)
}
