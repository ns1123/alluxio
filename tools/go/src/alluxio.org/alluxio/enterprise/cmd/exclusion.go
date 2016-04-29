package cmd

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"strings"
	"os"
	"path/filepath"
)

const (
	exclusionsFileName = ".enterprise"
)

func readExclusions(dirname string) (map[string]struct{}, error) {
	result := map[string]struct{}{}
	data, err := ioutil.ReadFile(filepath.Join(dirname, exclusionsFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return result, nil
		}
		return nil, err
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		result[strings.TrimRight(scanner.Text(), "/")] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
