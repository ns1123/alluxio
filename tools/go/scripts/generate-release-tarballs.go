package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

const versionMarker = "${VERSION}"

var releaseDistributions = map[string]string{
	"hadoop1.0": "1.0.4",
	"hadoop2.2": "2.2.0",
	"hadoop2.4": "2.4.1",
	"hadoop2.6": "2.6.0",
	"hadoop2.7": "2.7.2",
	"cdh4":      "2.0.0-mr1-cdh4.1.2",
	"cdh5":      "2.6.0-cdh5.4.8",
	"hdp2.0":    "2.2.0.2.0.6.3-7",
	"hdp2.1":    "2.4.0.2.1.7.4-3",
	"hdp2.2":    "2.6.0.2.2.9.1-1",
	"hdp2.3":    "2.7.1.2.3.99.0-195",
	"hdp2.4":    "2.7.1.2.4.0.0-169",
	"hdp2.5":    "2.7.3.2.5.0.0-1245",
	"mapr4.1":   "2.5.1-mapr-1503",
	"mapr5.0":   "2.7.0-mapr-1506",
	"mapr5.1":   "2.7.0-mapr-1602",
	"mapr5.2":   "2.7.0-mapr-1607",
}

var (
	debugFlag            bool
	distributionsFlag    string
	licenseCheckFlag     bool
	licenseSecretKeyFlag string
)

func init() {
	// Override default usage which isn't designed for scripts intended to be run by `go run`.
	flag.Usage = func() {
		fmt.Printf("Usage: go run generate-tarballs.go [options]\n")
		flag.PrintDefaults()
	}

	flag.BoolVar(&debugFlag, "debug", false, "whether to run in debug mode to generate additional console output")
	flag.StringVar(&distributionsFlag, "distributions", strings.Join(validDistributions(), ","), fmt.Sprintf("a comma-separated list of distributions to generate; the default is to generate all distributions"))
	flag.BoolVar(&licenseCheckFlag, "license-check", false, "whether the generated distribution should perform license checks")
	flag.StringVar(&licenseSecretKeyFlag, "license-secret-key", "", "the cryptographic key to use for license checks. Only applicable when using license-check")
	flag.Parse()
}

func validDistributions() []string {
	result := []string{}
	for t := range releaseDistributions {
		result = append(result, t)
	}
	sort.Strings(result)
	return result
}

func run(desc, cmd string, args ...string) string {
	fmt.Printf("  %s ... ", desc)
	if debugFlag {
		fmt.Printf("\n    command: %s %s ... ", cmd, strings.Join(args, " "))
	}
	c := exec.Command(cmd, args...)
	stderr := &bytes.Buffer{}
	stdout := &bytes.Buffer{}
	c.Stderr = stderr
	c.Stdout = stdout
	if err := c.Run(); err != nil {
		fmt.Printf("\"%v %v\" failed: %v\nstderr: <%v>\nstdout: <%v>\n", cmd, strings.Join(args, " "), err, stderr.String(), stdout.String())
		os.Exit(1)
	}
	fmt.Println("done")
	return stdout.String()
}

func generateTarballs() error {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("Failed to determine file of the go script")
	}
	goScriptsDir := filepath.Dir(file)
	generateTarballScript := filepath.Join(goScriptsDir, "generate-tarball.go")

	var distributions []string
	if distributionsFlag != "" {
		distributions = strings.Split(distributionsFlag, ",")
	} else {
		distributions = validDistributions()
	}
	for _, distribution := range distributions {
		hadoopVersion, ok := releaseDistributions[distribution]
		if !ok {
			fmt.Fprintf(os.Stderr, "distribution %s not recognized\n", distribution)
			continue
		}
		tarball := fmt.Sprintf("alluxio-%v-%v.tar.gz", versionMarker, hadoopVersion)
		args := []string{
			"-mvn-args", fmt.Sprintf("\"-Dhadoop.version=%v\"", hadoopVersion),
			"-target", tarball,
			"-license-check", fmt.Sprintf("%v", licenseCheckFlag),
		}
		if licenseCheckFlag {
			args = append(args, "-license-secret-key", licenseSecretKeyFlag)
		}
		run(fmt.Sprintf("Generating distribution for %v-%v at %v", distribution, hadoopVersion, tarball), "go", "run", generateTarballScript)
	}
	return nil
}

func main() {
	if err := generateTarballs(); err != nil {
		fmt.Printf("Failed to generate tarballs: %v", err)
		os.Exit(1)
	}
}
