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
	"cdh5.4":    "2.6.0-cdh5.4.9",
	"cdh5.6":    "2.6.0-cdh5.6.1",
	"cdh5.8":    "2.6.0-cdh5.8.5",
	"hdp2.0":    "2.2.0.2.0.6.3-7",
	"hdp2.1":    "2.4.0.2.1.7.4-3",
	"hdp2.2":    "2.6.0.2.2.9.18-1",
	"hdp2.3":    "2.7.1.2.3.99.0-195",
	"hdp2.4":    "2.7.1.2.4.4.1-9",
	"hdp2.5":    "2.7.3.2.5.5.5-2",
	"mapr4.1":   "2.5.1-mapr-1503",
	"mapr5.0":   "2.7.0-mapr-1506",
	"mapr5.1":   "2.7.0-mapr-1602",
	"mapr5.2":   "2.7.0-mapr-1607",
}

// TODO(andrew): consolidate the following definition with the duplicated definition in generate-tarball.go
// Map from UFS module name to a bool indicating if this module is included by default
var ufsModules = map[string]bool{
	"ufs-hadoop-1.0":     false,
	"ufs-hadoop-1.2":     true,
	"ufs-hadoop-2.2":     true,
	"ufs-hadoop-2.3":     false,
	"ufs-hadoop-2.4":     false,
	"ufs-hadoop-2.5":     false,
	"ufs-hadoop-2.6":     false,
	"ufs-hadoop-2.7":     true,
	"ufs-hadoop-2.8":     false,
	"ufs-hadoop-cdh5.6":  false,
	"ufs-hadoop-cdh5.8":  true,
	"ufs-hadoop-cdh5.11": false,
	"ufs-hadoop-hdp2.4":  false,
	"ufs-hadoop-hdp2.5":  true,
	"ufs-hadoop-mapr5.2": true,
}

var (
	debugFlag            bool
	distributionsFlag    string
	licenseCheckFlag     bool
	licenseSecretKeyFlag string
	nativeFlag           bool
	proxyURLFlag         string
	ufsModulesFlag       string
)

func init() {
	// Override default usage which isn't designed for scripts intended to be run by `go run`.
	flag.Usage = func() {
		fmt.Printf("Usage: go run generate-release-tarballs.go [options]\n")
		flag.PrintDefaults()
	}

	flag.BoolVar(&debugFlag, "debug", false, "whether to run in debug mode to generate additional console output")
	flag.StringVar(&distributionsFlag, "distributions", strings.Join(validDistributions(), ","), "a comma-separated list of distributions to generate; the default is to generate all distributions")
	flag.BoolVar(&licenseCheckFlag, "license-check", false, "whether the generated distribution should perform license checks")
	flag.StringVar(&licenseSecretKeyFlag, "license-secret-key", "", "the cryptographic key to use for license checks. Only applicable when using license-check")
	flag.BoolVar(&nativeFlag, "native", false, "whether to build the native Alluxio libraries. See core/client/fs/src/main/native/README.md for details.")
	flag.StringVar(&proxyURLFlag, "proxy-url", "", "the URL used for communicating with company backend")
	flag.StringVar(&ufsModulesFlag, "ufs-modules", strings.Join(defaultUfsModules(), ","),
		fmt.Sprintf("a comma-separated list of ufs modules to compile into the distribution tarball(s). Specify 'all' to build all ufs modules. Supported ufs modules: [%v]", strings.Join(validUfsModules(), ",")))
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

func validUfsModules() []string {
	result := []string{}
	for ufsModule := range ufsModules {
		result = append(result, ufsModule)
	}
	sort.Strings(result)
	return result
}

func defaultUfsModules() []string {
	result := []string{}
	for ufsModule := range ufsModules {
		if ufsModules[ufsModule] {
			result = append(result, ufsModule)
		}
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
		// TODO(chaomin): maybe append the OS type if native is enabled.
		tarball := fmt.Sprintf("alluxio-%v-%v.tar.gz", versionMarker, distribution)
		mvnArgs := fmt.Sprintf("-Dhadoop.version=%v", hadoopVersion)
		generateTarballArgs := []string{
			"-mvn-args", mvnArgs,
			"-target", tarball,
			"-ufs-modules", ufsModulesFlag,
		}
		if nativeFlag {
			generateTarballArgs = append(generateTarballArgs, "-native")
		}
		if licenseCheckFlag {
			generateTarballArgs = append(generateTarballArgs, "-license-check")
			if licenseSecretKeyFlag {
				generateTarballArgs = append(generateTarballArgs, "-license-secret-key", licenseSecretKeyFlag)
			}
			if proxyURLFlag {
				generateTarballArgs = append(generateTarballArgs, "-proxy-url", proxyURLFlag)
			}
		}
		args := []string{"run", generateTarballScript}
		args = append(args, generateTarballArgs...)
		run(fmt.Sprintf("Generating distribution for %v-%v at %v", distribution, hadoopVersion, tarball), "go", args...)
	}
	return nil
}

func handleArgs() error {
	if flag.NArg() > 0 {
		return fmt.Errorf("Unrecognized arguments: %v", flag.Args())
	}
	return nil
}

func main() {
	if err := handleArgs(); err != nil {
		fmt.Printf("Problem reading arguments: %v\n", err)
		flag.Usage()
		os.Exit(1)
	}

	if err := generateTarballs(); err != nil {
		fmt.Printf("Failed to generate tarballs: %v\n", err)
		os.Exit(1)
	}
}
