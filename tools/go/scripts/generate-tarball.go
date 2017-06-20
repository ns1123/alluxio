package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
)

const edition = "enterprise"
const versionMarker = "${VERSION}"

// ufsModuleNames is a map from ufs profile to the name used in the generated tarball.
var ufsModuleNames = map[string]string{
	"ufs-hadoop-1.0": "hdfsx-apache1_0",
	"ufs-hadoop-1.2": "hdfsx-apache1_2",
	"ufs-hadoop-2.2": "hdfsx-apache2_2",
	"ufs-hadoop-2.3": "hdfsx-apache2_3",
	"ufs-hadoop-2.4": "hdfsx-apache2_4",
	"ufs-hadoop-2.5": "hdfsx-apache2_5",
	"ufs-hadoop-2.6": "hdfsx-apache2_6",
	"ufs-hadoop-2.7": "hdfsx-apache2_7",
	"ufs-hadoop-2.8": "hdfsx-apache2_8",
	"ufs-cdh-5.6":    "hdfsx-cdh5_6",
	"ufs-cdh-5.8":    "hdfsx-cdh5_8",
	"ufs-cdh-5.11":   "hdfsx-cdh5_11",
	"ufs-hdp-2.4":    "hdfsx-hdp2_4",
	"ufs-hdp-2.5":    "hdfsx-hdp2_5",
}

// TODO(andrew): consolidate the following definition with the duplicated definition in generate-release-tarball.go
// ufsModules is a map from UFS module name to a bool indicating if the module is included by default.
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
}

var (
	callHomeFlag         bool
	callHomeBucketFlag   string
	debugFlag            bool
	licenseCheckFlag     bool
	licenseSecretKeyFlag string
	mvnArgsFlag          string
	nativeFlag           bool
	profilesFlag         string
	proxyURLFlag         string
	targetFlag           string
	ufsModulesFlag       string
)

// frameworks is a map from framework to whether building a client for the framework requires recompilation.
var frameworks = map[string]bool{
	"flink": false,
	"hadoop": false,
	"presto": true,
	"spark": true,
}
var webappDir = "core/server/common/src/main/webapp"
var webappWar = "assembly/webapp.war"

func init() {
	// Override default usage which isn't designed for scripts intended to be run by `go run`.
	flag.Usage = func() {
		fmt.Printf("Usage: go run generate-tarballs.go [options]\n")
		flag.PrintDefaults()
	}

	flag.BoolVar(&callHomeFlag, "call-home", false, "whether the generated distribution should perform call home")
	flag.StringVar(&callHomeBucketFlag, "call-home-bucket", "", "the S3 bucket the generated distribution should upload call home information to")
	flag.BoolVar(&debugFlag, "debug", false, "whether to run in debug mode to generate additional console output")
	flag.BoolVar(&licenseCheckFlag, "license-check", false, "whether the generated distribution should perform license checks")
	flag.StringVar(&licenseSecretKeyFlag, "license-secret-key", "", "the cryptographic key to use for license checks. Only applicable when using license-check")
	flag.StringVar(&mvnArgsFlag, "mvn-args", "", `a comma-separated list of additional Maven arguments to build with, e.g. -mvn-args "-Pspark,-Dhadoop.version=2.2.0"`)
	flag.BoolVar(&nativeFlag, "native", false, "whether to build the native Alluxio libraries. See core/client/fs/src/main/native/README.md for details.")
	flag.StringVar(&profilesFlag, "profiles", "", "[DEPRECATED: use -mvn-args instead] a comma-separated list of build profiles to use")
	flag.StringVar(&proxyURLFlag, "proxy-url", "", "the URL used for communicating with company backend")
	flag.StringVar(&targetFlag, "target", fmt.Sprintf("alluxio-%v.tar.gz", versionMarker),
		fmt.Sprintf("an optional target name for the generated tarball. The default is alluxio-%v.tar.gz. The string %q will be substituted with the built version. "+
			`Note that trailing ".tar.gz" will be stripped to determine the name for the root directory of the generated tarball`, versionMarker, versionMarker))
	flag.StringVar(&ufsModulesFlag, "ufs-modules", strings.Join(defaultUfsModules(), ","),
		fmt.Sprintf("a comma-separated list of ufs modules to compile into the distribution tarball. Specify 'all' to build all ufs modules. Supported ufs modules: [%v]", strings.Join(validUfsModules(), ",")))
	flag.Parse()
}

func validUfsModules() []string {
	result := []string{}
	for t := range ufsModuleNames {
		result = append(result, t)
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

func replace(path, old, new string) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ReadFile() failed: %v\n", err)
		os.Exit(1)
	}
	data = bytes.Replace(data, []byte(old), []byte(new), -1)
	if err := ioutil.WriteFile(path, data, os.FileMode(0644)); err != nil {
		fmt.Fprintf(os.Stderr, "WriteFile() failed: %v\n", err)
		os.Exit(1)
	}
}

func mkdir(path string) {
	if err := os.MkdirAll(path, os.FileMode(0755)); err != nil {
		fmt.Fprintf(os.Stderr, "MkdirAll() failed: %v\n", err)
		os.Exit(1)
	}
}

func chdir(path string) {
	if err := os.Chdir(path); err != nil {
		fmt.Fprintf(os.Stderr, "Chdir() failed: %v\n", err)
		os.Exit(1)
	}
}

func getCommonMvnArgs() []string {
	args := []string{"clean", "install", "-DskipTests", "-Dfindbugs.skip", "-Dmaven.javadoc.skip", "-Dcheckstyle.skip", "-Pmesos"}
	if profilesFlag != "" {
		for _, profile := range strings.Split(profilesFlag, ",") {
			args = append(args, fmt.Sprintf("-P%s", profile))
		}
	}
	if mvnArgsFlag != "" {
		for _, arg := range strings.Split(mvnArgsFlag, ",") {
			args = append(args, arg)
		}
	}
	if nativeFlag {
		args = append(args, "-Pnative")
	}
	if callHomeFlag {
		args = append(args, "-Dcall.home.enabled=true")
		if callHomeBucketFlag != "" {
			args = append(args, fmt.Sprintf("-Dcall.home.bucket=%s", callHomeBucketFlag))
		}
	}
	if licenseCheckFlag {
		args = append(args, "-Dlicense.check.enabled=true")
		if licenseSecretKeyFlag != "" {
			args = append(args, fmt.Sprintf("-Dlicense.secret.key=%s", licenseSecretKeyFlag))
		}
	}
	if proxyURLFlag != "" {
		args = append(args, fmt.Sprintf("-Dproxy.url=%s", proxyURLFlag))
	}
	return args
}

func getVersion() (string, error) {
	versionLine := run("grepping for the version", "grep", "-m1", "<version>", "pom.xml")
	re := regexp.MustCompile(".*<version>(.*)</version>.*")
	match := re.FindStringSubmatch(versionLine)
	if len(match) < 2 {
		return "", errors.New("Failed to find version")
	}
	return match[1], nil
}

func addAdditionalFiles(srcPath, dstPath, version string) {
	chdir(srcPath)
	pathsToCopy := []string {
		// BIN
		"bin/alluxio",
		"bin/alluxio-masters.sh",
		"bin/alluxio-mount.sh",
		"bin/alluxio-start.sh",
		"bin/alluxio-stop.sh",
		"bin/alluxio-workers.sh",
		// CONF
		"conf/alluxio-env.sh.template",
		"conf/alluxio-site.properties.template",
		"conf/core-site.xml.template",
		"conf/log4j.properties",
		"conf/masters",
		"conf/metrics.properties.template",
		"conf/workers",
		// LIBEXEC
		"libexec/alluxio-config.sh",
		"libexec/alluxio-layout.sh.template",
		// DOCKER
		"integration/docker/bin/alluxio-master.sh",
		"integration/docker/bin/alluxio-job-master.sh",
		"integration/docker/bin/alluxio-job-worker.sh",
		"integration/docker/bin/alluxio-proxy.sh",
		"integration/docker/bin/alluxio-worker.sh",
		// MESOS
		"integration/mesos/bin/alluxio-env-mesos.sh",
		"integration/mesos/bin/alluxio-master-mesos.sh",
		"integration/mesos/bin/alluxio-mesos-start.sh",
		"integration/mesos/bin/alluxio-worker-mesos.sh",
		"integration/mesos/bin/common.sh",
	}
	for _, path := range pathsToCopy {
		mkdir(filepath.Join(dstPath, filepath.Dir(path)))
		run(fmt.Sprintf("adding %v", path), "mv", path, filepath.Join(dstPath, path))
	}
	// DOCKER
	mkdir(filepath.Join(dstPath, "integration/docker/conf"))
	// Copy files from /docker-enterprise to /docker.
	for _, file := range []string{
		"Dockerfile",
		"README.md",
		"entrypoint.sh",
	} {
		src := filepath.Join("integration/docker-enterprise", file)
		dst := filepath.Join("integration/docker", file)
		run(fmt.Sprintf("adding %v", src), "mv", src, filepath.Join(dstPath, dst))
	}
	// UFS MODULES
	mkdir(filepath.Join(dstPath, "lib"))
	for _, module := range strings.Split(ufsModulesFlag, ",") {
		moduleName, ok := ufsModuleNames[module]
		if !ok {
			// This should be impossible, we validate ufsModulesFlag at the start.
			fmt.Fprintf(os.Stderr, "Unrecognized ufs module: %v", module)
			os.Exit(1)
		}
		ufsJar := fmt.Sprintf("alluxio-underfs-%v-%v.jar", moduleName, version)
		run(fmt.Sprintf("adding ufs module %v to lib/", module), "mv", filepath.Join(srcPath, "lib", ufsJar), filepath.Join(dstPath, "lib"))
	}
	// NATIVE LIBRARIES
	if nativeFlag {
		run("adding Alluxio native libraries", "mv", fmt.Sprintf("lib/native"), filepath.Join(dstPath, "lib", "native"))
	}
}

func generateTarball() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("Failed to determine file of the go script")
	}
	goScriptsDir := filepath.Dir(file)
	repoPath := filepath.Join(goScriptsDir, "../../../")
	srcPath, err := ioutil.TempDir("", edition)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to create temp directory: %v", err))
	}
	run(fmt.Sprintf("copying source from %v to %v", repoPath, srcPath), "cp", "-R", repoPath+"/.", srcPath)
	chdir(srcPath)
	run("running git clean -fdx", "git", "clean", "-fdx")

	// GET THE VERSION AND PREPEND WITH `edition-`
	originalVersion, err := getVersion()
	if err != nil {
		return err
	}
	version := edition + "-" + originalVersion
	run("updating version to "+version, "mvn", "versions:set", "-DnewVersion="+version, "-DgenerateBackupPoms=false")
	run("updating version to "+version+" in alluxio-config.sh", "sed", "-i.bak", fmt.Sprintf("s/%v/%v/g", originalVersion, version), "libexec/alluxio-config.sh")

	// OVERRIDE DEFAULT SETTINGS
	// Update the web app location.
	replace("core/common/src/main/java/alluxio/PropertyKey.java", webappDir, webappWar)
	// Update the assembly jar paths.
	replace("libexec/alluxio-config.sh", "assembly/client/target/alluxio-assembly-client-${VERSION}-jar-with-dependencies.jar", "assembly/alluxio-client-${VERSION}.jar")
	replace("libexec/alluxio-config.sh", "assembly/server/target/alluxio-assembly-server-${VERSION}-jar-with-dependencies.jar", "assembly/alluxio-server-${VERSION}.jar")

	// COMPILE
	mvnArgs := getCommonMvnArgs()
	// Only add ufs modules for the main build, not for building per-framework clients.
	for _, module := range strings.Split(ufsModulesFlag, ",") {
		mvnArgs = append(mvnArgs, fmt.Sprintf("-P%v", module))
	}
	run("compiling repo", "mvn", mvnArgs...)

	// SET DESTINATION PATHS
	tarball := strings.Replace(targetFlag, versionMarker, version, 1)
	dstDir := strings.TrimSuffix(filepath.Base(tarball), ".tar.gz")
	dstPath := filepath.Join(cwd, dstDir)
	run(fmt.Sprintf("removing any existing %v", dstPath), "rm", "-rf", dstPath)
	fmt.Printf("Creating %s:\n", tarball)

	// CREATE NEEDED DIRECTORIES
	// Create the directory for the server jar.
	mkdir(filepath.Join(dstPath, "assembly"))
	// Create directories for the client jars.
	mkdir(filepath.Join(dstPath, "client/default"))
	for framework, _ := range frameworks {
		mkdir(filepath.Join(dstPath, "client", framework))
	}
	mkdir(filepath.Join(dstPath, "logs"))

	// ADD ALLUXIO ASSEMBLY JARS TO DISTRIBUTION
	run("adding Alluxio client assembly jar", "mv", fmt.Sprintf("assembly/client/target/alluxio-assembly-client-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "assembly", fmt.Sprintf("alluxio-client-%v.jar", version)))
	run("adding Alluxio server assembly jar", "mv", fmt.Sprintf("assembly/server/target/alluxio-assembly-server-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "assembly", fmt.Sprintf("alluxio-server-%v.jar", version)))
	// Condense the webapp into a single .war file.
	run("jarring up webapp", "jar", "-cf", filepath.Join(dstPath, webappWar), "-C", webappDir, ".")

	// ADD ADDITIONAL CONTENT TO DISTRIBUTION
	addAdditionalFiles(srcPath, dstPath, version)

	// BUILD ALLUXIO CLIENTS JARS AND ADD THEM TO DISTRIBUTION
	chdir(filepath.Join(srcPath, "core/client/runtime"))
	run("building Alluxio default client jar", "mvn", getCommonMvnArgs()...)
	run("adding Alluxio default client jar", "mv", fmt.Sprintf("target/alluxio-core-client-runtime-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "client", fmt.Sprintf("default/alluxio-%v-default-client.jar", version)))
	for framework, recompile := range frameworks {
		if recompile {
			clientArgs := getCommonMvnArgs()
			clientArgs = append(clientArgs, fmt.Sprintf("-P%s", framework))
			run(fmt.Sprintf("building Alluxio %s client jar", framework), "mvn", clientArgs...)
			run(fmt.Sprintf("adding Alluxio %s client jar", framework), "mv", fmt.Sprintf("target/alluxio-core-client-runtime-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "client", fmt.Sprintf("%v/alluxio-%v-%v-client.jar", framework, version, framework)))
		} else {
			run(fmt.Sprintf("creating symlink for %v client jar", framework), "ln", "-s", fmt.Sprintf("../default/alluxio-%v-default-client.jar", version), filepath.Join(dstPath, "client", fmt.Sprintf("%v/alluxio-%v-%v-client.jar", framework, version, framework)))
		}
	}

	// CREATE DISTRIBUTION TARBALL AND CLEANUP
	chdir(cwd)
	run("creating the distribution tarball", "tar", "-czvf", tarball, dstDir)
	run("removing the repository", "rm", "-rf", srcPath, dstPath)

	return nil
}

func validateUfsModulesFlag() error {
	if strings.ToLower(ufsModulesFlag) == "all" {
		return nil
	}

	for _, module := range strings.Split(ufsModulesFlag, ",") {
		if _, ok := ufsModuleNames[module]; !ok {
			return fmt.Errorf("ufs module %v not recognized", module)
		}
	}
	return nil
}

func handleArgs() error {
	if flag.NArg() > 0 {
		return fmt.Errorf("Unrecognized arguments: %v", flag.Args())
	}
	if err := validateUfsModulesFlag(); err != nil {
		return err
	}
	if strings.ToLower(ufsModulesFlag) == "all" {
		ufsModulesFlag = strings.Join(validUfsModules(), ",")
	}
	return nil
}

func main() {
	if err := handleArgs(); err != nil {
		fmt.Printf("Problem reading arguments: %v\n", err)
		flag.Usage()
		os.Exit(1)
	}

	if err := generateTarball(); err != nil {
		fmt.Printf("Failed to generate tarball: %v\n", err)
		os.Exit(1)
	}
}
