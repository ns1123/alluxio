package cmd

import (
	"bufio"
	"bytes"
	"fmt"
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

type warning struct {
	filename string
	line     int
	message  string
}

func lint(filename string, warnings map[string][]warning) error {
	ft := inferFileType(filename)
	if ft == unknownType {
		// skip unknown types
		return nil
	}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	sm, line := newStateMachine(filename), 0
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line++
		for _, warning := range sm.next(strings.TrimSpace(scanner.Text()), line, ft) {
			warnings[filename] = append(warnings[filename], warning)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	// generate warnings if some annotations have not been completed
	for _, warning := range sm.warning() {
		warnings[filename] = append(warnings[filename], warning)
	}
	return nil
}

func treeWalk(dirname string, revisionedObjects *tree, warnings map[string][]warning) error {
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
			if err := treeWalk(filepath.Join(dirname, info.Name()), revisionedObjects.get(info.Name()), warnings); err != nil {
				return err
			}
		} else {
			if err := lint(filepath.Join(dirname, info.Name()), warnings); err != nil {
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
	warnings := map[string][]warning{}
	if err := treeWalk(flagRepo, tree, warnings); err != nil {
		return err
	}
	for filename, fileWarnings := range warnings {
		fmt.Printf("warnings for %v:\n", filename)
		for _, w := range fileWarnings {
			fmt.Printf("  line %d: %v\n", w.line, w.message)
		}
	}
	if len(warnings) != 0 && flagWarning {
		return fmt.Errorf("warnings encountered")
	}
	return nil
}
