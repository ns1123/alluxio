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

// Map from ufs profile to the name used in the generated tarball.
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
}

var (
	debugFlag            bool
	licenseCheckFlag     bool
	licenseSecretKeyFlag string
	mvnArgsFlag          string
	nativeFlag           bool
	profilesFlag         string
	targetFlag           string
	ufsModulesFlag       string
)

var frameworks = []string{"flink", "hadoop", "spark"}
var webappDir = "core/server/common/src/main/webapp"
var webappWar = "assembly/webapp.war"

func init() {
	// Override default usage which isn't designed for scripts intended to be run by `go run`.
	flag.Usage = func() {
		fmt.Printf("Usage: go run generate-tarballs.go [options]\n")
		flag.PrintDefaults()
	}

	flag.BoolVar(&debugFlag, "debug", false, "whether to run in debug mode to generate additional console output")
	flag.BoolVar(&licenseCheckFlag, "license-check", false, "whether the generated distribution should perform license checks")
	flag.StringVar(&licenseSecretKeyFlag, "license-secret-key", "", "the cryptographic key to use for license checks. Only applicable when using license-check")
	flag.StringVar(&mvnArgsFlag, "mvn-args", "", `a comma-separated list of additional Maven arguments to build with, e.g. -mvn-args "-Pspark,-Dhadoop.version=2.2.0"`)
	flag.BoolVar(&nativeFlag, "native", false, "whether to build the native Alluxio libraries. See core/client/fs/src/main/native/README.md for details.")
	flag.StringVar(&profilesFlag, "profiles", "", "[DEPRECATED: use -mvn-args instead] a comma-separated list of build profiles to use")
	flag.StringVar(&targetFlag, "target", fmt.Sprintf("alluxio-%v.tar.gz", versionMarker),
		fmt.Sprintf("an optional target name for the generated tarball. The default is alluxio-%v.tar.gz. The string %q will be substituted with the built version. "+
			`Note that trailing ".tar.gz" will be stripped to determine the name for the root directory of the generated tarball`, versionMarker, versionMarker))
	flag.StringVar(&ufsModulesFlag, "ufs-modules", "ufs-hadoop-2.2", fmt.Sprintf("a comma-separated list of ufs modules to compile into the distribution tarball. Specify 'all' to build all ufs modules. Supported ufs modules: [%v]", strings.Join(validUfsModules(), ",")))
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
	if licenseCheckFlag {
		args = append(args, "-Dlicense.check.enabled=true")
		if licenseSecretKeyFlag != "" {
			args = append(args, fmt.Sprintf("-Dlicense.secret.key=%s", licenseSecretKeyFlag))
		}
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
	run("adding Alluxio scripts", "mv", "bin", "conf", "libexec", dstPath)
	// DOCKER
	mkdir(filepath.Join(dstPath, "integration/docker/bin"))
	for _, file := range []string{
		"alluxio-master.sh",
		"alluxio-job-master.sh",
		"alluxio-job-worker.sh",
		"alluxio-proxy.sh",
		"alluxio-worker.sh",
	} {
		path := filepath.Join("integration/docker/bin", file)
		run(fmt.Sprintf("adding %v", path), "mv", path, filepath.Join(dstPath, path))
	}
	// cp -r docker-enterprise/. to copy the contents of the directory, not the directory itself.
	run("copying docker-enterprise directory", "cp", "-r", "integration/docker-enterprise/.", filepath.Join(dstPath, "integration/docker"))
	// MESOS
	mkdir(filepath.Join(dstPath, "integration", "mesos", "bin"))
	for _, file := range []string{
		"alluxio-env-mesos.sh",
		"alluxio-master-mesos.sh",
		"alluxio-mesos-start.sh",
		"alluxio-worker-mesos.sh",
		"common.sh",
	} {
		path := filepath.Join("integration/mesos/bin", file)
		run(fmt.Sprintf("adding %v", path), "mv", path, filepath.Join(dstPath, path))
	}
	// UFS MODULES
	mkdir(filepath.Join(dstPath, "lib"))
	modules := validUfsModules()
	for _, module := range modules {
		moduleName, ok := ufsModuleNames[module]
		if !ok {
			// This should be impossible, we validate ufsModulesFlag at the start.
			fmt.Fprintf(os.Stderr, "Unrecognized ufs module: %v", module)
			os.Exit(1)
		}
		ufsJar := fmt.Sprintf("alluxio-underfs-%v-%v.jar", moduleName, version)
		run(fmt.Sprintf("Add ufs module %v to lib/", module), "mv", filepath.Join(srcPath, "lib", ufsJar), filepath.Join(dstPath, "lib"))
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
	run("Running git clean -fdx", "git", "clean", "-fdx")

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
	for _, framework := range frameworks {
		mkdir(filepath.Join(dstPath, "client", framework))
	}
	mkdir(filepath.Join(dstPath, "logs"))

	// ADD ALLUXIO ASSEMBLY JARS TO DISTRIBUTION
	run("adding Alluxio client assembly jar", "mv", fmt.Sprintf("assembly/client/target/alluxio-assembly-client-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "assembly", fmt.Sprintf("alluxio-client-%v.jar", version)))
	run("adding Alluxio server assembly jar", "mv", fmt.Sprintf("assembly/server/target/alluxio-assembly-server-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "assembly", fmt.Sprintf("alluxio-server-%v.jar", version)))
	// Condense the webapp into a single .war file.
	run("jarring up webapp", "jar", "-cf", filepath.Join(dstPath, webappWar), "-C", webappDir, ".")
	if nativeFlag {
		run("adding Alluxio native libraries", "mv", fmt.Sprintf("lib/"), filepath.Join(dstPath, "lib/"))
	}

	// ADD ADDITIONAL CONTENT TO DISTRIBUTION
	addAdditionalFiles(srcPath, dstPath, version)

	// BUILD ALLUXIO CLIENTS JARS AND ADD THEM TO DISTRIBUTION
	chdir(filepath.Join(srcPath, "core/client/runtime"))
	for _, framework := range frameworks {
		clientArgs := getCommonMvnArgs()
		if framework != "hadoop" {
			clientArgs = append(clientArgs, fmt.Sprintf("-P%s", framework))
		}
		run(fmt.Sprintf("building Alluxio %s client jar", framework), "mvn", clientArgs...)
		run(fmt.Sprintf("adding Alluxio %s client jar", framework), "mv", fmt.Sprintf("target/alluxio-core-client-runtime-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "client", fmt.Sprintf("%v/alluxio-%v-%v-client.jar", framework, version, framework)))
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
