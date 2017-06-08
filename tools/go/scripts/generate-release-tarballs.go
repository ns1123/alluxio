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

var releaseDistributions = map[string]interface{}{
	"hadoop-1.0": nil,
	"hadoop-1.2": nil,
	"hadoop-2.2": nil,
	"hadoop-2.3": nil,
	"hadoop-2.4": nil,
	"hadoop-2.5": nil,
	"hadoop-2.6": nil,
	"hadoop-2.7": nil,
	"hadoop-2.8": nil,
	"cdh-4.1":    nil,
	"cdh-5.4":    nil,
	"cdh-5.6":    nil,
	"cdh-5.8":    nil,
	"hdp-2.0":    nil,
	"hdp-2.1":    nil,
	"hdp-2.2":    nil,
	"hdp-2.3":    nil,
	"hdp-2.4":    nil,
	"hdp-2.5":    nil,
	"mapr-4.1":   nil,
	"mapr-5.0":   nil,
	"mapr-5.1":   nil,
	"mapr-5.2":   nil,
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
	callHomeFlag         bool
	callHomeBucketFlag   string
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

	flag.BoolVar(&callHomeFlag, "call-home", false, "whether the generated distribution should perform call home")
	flag.StringVar(&callHomeBucketFlag, "call-home-bucket", "", "the S3 bucket the generated distribution should upload call home information to")
	flag.BoolVar(&debugFlag, "debug", false, "whether to run in debug mode to generate additional console output")
	flag.StringVar(&distributionsFlag, "distributions", strings.Join(validReleaseDistributions(), ","), "a comma-separated list of distributions to generate; the default is to generate all distributions")
	flag.BoolVar(&licenseCheckFlag, "license-check", false, "whether the generated distribution should perform license checks")
	flag.StringVar(&licenseSecretKeyFlag, "license-secret-key", "", "the cryptographic key to use for license checks. Only applicable when using license-check")
	flag.BoolVar(&nativeFlag, "native", false, "whether to build the native Alluxio libraries. See core/client/fs/src/main/native/README.md for details.")
	flag.StringVar(&proxyURLFlag, "proxy-url", "", "the URL used for communicating with company backend")
	flag.StringVar(&ufsModulesFlag, "ufs-modules", strings.Join(defaultUfsModules(), ","),
		fmt.Sprintf("a comma-separated list of ufs modules to compile into the distribution tarball(s). Specify 'all' to build all ufs modules. Supported ufs modules: [%v]", strings.Join(validUfsModules(), ",")))
	flag.Parse()
}

func validReleaseDistributions() []string {
	result := []string{}
	for distribution, _ := range releaseDistributions {
		result = append(result, distribution)
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
		distributions = validReleaseDistributions()
	}
	for _, distribution := range distributions {
		// TODO(chaomin): maybe append the OS type if native is enabled.
		tarball := fmt.Sprintf("alluxio-%v-%v.tar.gz", versionMarker, distribution)
		generateTarballArgs := []string{
			"-mvn-args", fmt.Sprintf("-P%v", distribution),
			"-target", tarball,
			"-ufs-modules", ufsModulesFlag,
		}
		if nativeFlag {
			generateTarballArgs = append(generateTarballArgs, "-native")
		}
		if callHomeFlag {
			generateTarballArgs = append(generateTarballArgs, "-call-home")
			if callHomeBucketFlag != "" {
				generateTarballArgs = append(generateTarballArgs, "-call-home-bucket", callHomeBucketFlag)
			}
		}
		if licenseCheckFlag {
			generateTarballArgs = append(generateTarballArgs, "-license-check")
			if licenseSecretKeyFlag != "" {
				generateTarballArgs = append(generateTarballArgs, "-license-secret-key", licenseSecretKeyFlag)
			}
		}
		if proxyURLFlag != "" {
			generateTarballArgs = append(generateTarballArgs, "-proxy-url", proxyURLFlag)
		}
		args := []string{"run", generateTarballScript}
		args = append(args, generateTarballArgs...)
		run(fmt.Sprintf("Generating distribution for profile %v at %v", distribution, tarball), "go", args...)
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
