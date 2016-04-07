package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"
)

type tree struct {
	m map[string]*tree
}

func (t *tree) contains(path string) bool {
	handle := t
	for _, component := range strings.Split(path, string(filepath.Separator)) {
		if handle == nil {
			return false
		}
		object, ok := handle.m[component]
		if !ok {
			return false
		}
		handle = object
	}
	return true
}

func (t *tree) get(component string) *tree {
	return t.m[component]
}

func (t *tree) insert(path string) error {
	handle, components := t, strings.Split(path, string(filepath.Separator))
	for i, component := range components {
		if handle == nil {
			return fmt.Errorf("conflicting entries")
		}
		object, ok := handle.m[component]
		if !ok {
			if handle.m == nil {
				handle.m = map[string]*tree{}
			}
			if i == len(components)-1 {
				handle.m[component] = nil // file
			} else {
				handle.m[component] = &tree{} // directory
			}
			object = handle.m[component]
		}
		handle = object
	}
	return nil
}

func (t *tree) walk(dirname string, fn func(string) error) error {
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
		if !t.contains(info.Name()) {
			continue // skip unrevisioned objects
		}
		if info.IsDir() {
			if err := t.get(info.Name()).walk(filepath.Join(dirname, info.Name()), fn); err != nil {
				return err
			}
		} else {
			if err := fn(filepath.Join(dirname, info.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func newTree(dirname string) (*tree, error) {
	cmd := exec.Command("git", "ls-files")
	data, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	result := &tree{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		result.insert(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
