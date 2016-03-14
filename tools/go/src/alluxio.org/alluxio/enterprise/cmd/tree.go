package cmd

import (
	"bufio"
	"bytes"
	"fmt"
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
