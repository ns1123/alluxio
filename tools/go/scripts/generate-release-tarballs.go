package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

const versionMarker = "${VERSION}"

var versionRE = regexp.MustCompile("^(\\d+)\\.(\\d+)\\.(\\d+)(.*)?$")

type version struct {
	major  int
	minor  int
	patch  int
	suffix string
}

func ParseVersion(v string) version {
	matches := versionRE.FindStringSubmatch(v)
	major, err := strconv.Atoi(matches[1])
	if err != nil {
		panic(fmt.Sprintf("failed to parse %v as number", matches[1]))
	}
	minor, err := strconv.Atoi(matches[2])
	if err != nil {
		panic(fmt.Sprintf("failed to parse %v as number", matches[2]))
	}
	patch, err := strconv.Atoi(matches[3])
	if err != nil {
		panic(fmt.Sprintf("failed to parse %v as number", matches[3]))
	}
	return version{
		major:  major,
		minor:  minor,
		patch:  patch,
		suffix: matches[4],
	}
}

func (v version) String() string {
	return fmt.Sprintf("%v.%v.%v%v", v.major, v.minor, v.patch, v.suffix)
}

func (v version) HadoopProfile() string {
	switch v.major {
	case 1:
		return "hadoop-1"
	case 2:
		return "hadoop-2"
	default:
		panic(fmt.Sprintf("unexpected hadoop major version %v", v.major))
	}
}

func (v version) HasHadoopKMS() bool {
	return v.major > 2 || (v.major == 2 && v.minor > 5)
}

// hadoopDistributions maps hadoop distributions to versions
var hadoopDistributions = map[string]version{
	"hadoop-1.0": ParseVersion("1.0.4"),
	"hadoop-1.2": ParseVersion("1.2.1"),
	"hadoop-2.2": ParseVersion("2.2.0"),
	"hadoop-2.3": ParseVersion("2.3.0"),
	"hadoop-2.4": ParseVersion("2.4.1"),
	"hadoop-2.5": ParseVersion("2.5.2"),
	"hadoop-2.6": ParseVersion("2.6.5"),
	"hadoop-2.7": ParseVersion("2.7.3"),
	"hadoop-2.8": ParseVersion("2.8.0"),
	"cdh-4.1":    ParseVersion("2.0.0-mr1-cdh4.1.2"),
	"cdh-5.4":    ParseVersion("2.6.0-cdh5.4.9"),
	"cdh-5.6":    ParseVersion("2.6.0-cdh5.6.1"),
	"cdh-5.8":    ParseVersion("2.6.0-cdh5.8.5"),
	"hdp-2.0":    ParseVersion("2.2.0.2.0.6.3-7"),
	"hdp-2.1":    ParseVersion("2.4.0.2.1.7.4-3"),
	"hdp-2.2":    ParseVersion("2.6.0.2.2.9.18-1"),
	"hdp-2.3":    ParseVersion("2.7.1.2.3.99.0-195"),
	"hdp-2.4":    ParseVersion("2.7.1.2.4.4.1-9"),
	"hdp-2.5":    ParseVersion("2.7.3.2.5.5.5-2"),
	"mapr-4.1":   ParseVersion("2.5.1-mapr-1503"),
	"mapr-5.0":   ParseVersion("2.7.0-mapr-1506"),
	"mapr-5.1":   ParseVersion("2.7.0-mapr-1602"),
	"mapr-5.2":   ParseVersion("2.7.0-mapr-1607"),
}

// TODO(andrew): consolidate the following definition with the duplicated definition in generate-tarball.go
// Map from UFS module name to a bool indicating if this module is included by default
var ufsModules = map[string]bool{
	"ufs-hadoop-1.0": false,
	"ufs-hadoop-1.2": true,
	"ufs-hadoop-2.2": true,
	"ufs-hadoop-2.3": false,
	"ufs-hadoop-2.4": false,
	"ufs-hadoop-2.5": false,
	"ufs-hadoop-2.6": false,
	"ufs-hadoop-2.7": true,
	"ufs-hadoop-2.8": false,
	"ufs-cdh-5.6":    false,
	"ufs-cdh-5.8":    true,
	"ufs-cdh-5.11":   false,
	"ufs-hdp-2.4":    false,
	"ufs-hdp-2.5":    true,
	"ufs-mapr-5.2":   true,
}

var (
	callHomeFlag            bool
	callHomeBucketFlag      string
	debugFlag               bool
	hadoopDistributionsFlag string
	licenseCheckFlag        bool
	licenseSecretKeyFlag    string
	nativeFlag              bool
	proxyURLFlag            string
	ufsModulesFlag          string
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
	flag.StringVar(&hadoopDistributionsFlag, "hadoop-distributions", strings.Join(validHadoopDistributions(), ","), "a comma-separated list of hadoop distributions to generate Alluxio distributions for")
	flag.BoolVar(&licenseCheckFlag, "license-check", false, "whether the generated distribution should perform license checks")
	flag.StringVar(&licenseSecretKeyFlag, "license-secret-key", "", "the cryptographic key to use for license checks. Only applicable when using license-check")
	flag.BoolVar(&nativeFlag, "native", false, "whether to build the native Alluxio libraries. See core/client/fs/src/main/native/README.md for details.")
	flag.StringVar(&proxyURLFlag, "proxy-url", "", "the URL used for communicating with company backend")
	flag.StringVar(&ufsModulesFlag, "ufs-modules", strings.Join(defaultUfsModules(), ","),
		fmt.Sprintf("a comma-separated list of ufs modules to compile into the distribution tarball(s). Specify 'all' to build all ufs modules. Supported ufs modules: [%v]", strings.Join(validUfsModules(), ",")))
	flag.Parse()
}

func validHadoopDistributions() []string {
	result := []string{}
	for distribution, _ := range hadoopDistributions {
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
	if hadoopDistributionsFlag != "" {
		distributions = strings.Split(hadoopDistributionsFlag, ",")
	} else {
		distributions = validHadoopDistributions()
	}
	for _, distribution := range distributions {
		version, ok := hadoopDistributions[distribution]
		if !ok {
			fmt.Fprintf(os.Stderr, "hadoop distribution %s not recognized\n", distribution)
			continue
		}
		// TODO(chaomin): maybe append the OS type if native is enabled.
		tarball := fmt.Sprintf("alluxio-%v-%v.tar.gz", versionMarker, distribution)
		mvnArgs := []string{fmt.Sprintf("-Dhadoop.version=%v", version), fmt.Sprintf("-P%v", version.HadoopProfile())}
		if version.HasHadoopKMS() {
			mvnArgs = append(mvnArgs, "-Phadoop-kms")
		}
		generateTarballArgs := []string{
			"-mvn-args", strings.Join(mvnArgs, ","),
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
		run(fmt.Sprintf("Generating distribution for %v at %v", distribution, tarball), "go", args...)
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
