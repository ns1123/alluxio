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
	"strings"
)

var (
	licenseCheckFlag     bool
	licenseSecretKeyFlag string
	profilesFlag         string
)

var frameworks = []string{"flink", "hadoop", "spark"}

func init() {
	// Override default usage which isn't designed for scripts intended to be run by `go run`.
	flag.Usage = func() {
		fmt.Printf("Usage: go run generate-tarballs.go [options]\n")
		flag.PrintDefaults()
	}

	flag.BoolVar(&licenseCheckFlag, "license-check", false, "whether the generated distribution should perform license checks")
	flag.StringVar(&licenseSecretKeyFlag, "license-secret-key", "", "the cryptographic key to use for license checks. Only applicable when using license-check")
	flag.StringVar(&profilesFlag, "profiles", "", fmt.Sprintf("a comma-separated list of build profiles to use"))
	flag.Parse()
}

func run(desc, cmd string, args ...string) string {
	fmt.Printf("  %s ... ", desc)
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

func getMvnArgs() []string {
	args := []string{"clean", "install", "-DskipTests", "-Dfindbugs.skip", "-Dmaven.javadoc.skip", "-Dcheckstyle.skip", "-Pmesos"}
	if profilesFlag != "" {
		for _, profile := range strings.Split(profilesFlag, ",") {
			args = append(args, fmt.Sprintf("-P%s", profile))
		}
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
	srcPath, err := ioutil.TempDir("", "enterprise")
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to create temp directory: %v", err))
	}
	run(fmt.Sprintf("copying source from %v to %v", repoPath, srcPath), "cp", "-R", repoPath+"/", srcPath)

	// GET THE VERSION AND PREPEND WITH `enterprise-`
	chdir(srcPath)
	originalVersion, err := getVersion()
	if err != nil {
		return err
	}
	version := "enterprise-" + originalVersion
	run("updating to enterprise- version", "mvn", "versions:set", "-DnewVersion="+version, "-DgenerateBackupPoms=false")

	// OVERRIDE DEFAULT SETTINGS
	// Update the web app location.
	replace("core/common/src/main/java/alluxio/PropertyKey.java", "core/server/src/main/webapp", fmt.Sprintf("server/alluxio-%v-server.jar", version))
	// Update the server jar path.
	replace("libexec/alluxio-config.sh", "assembly/target/alluxio-assemblies-${VERSION}-jar-with-dependencies.jar", "server/alluxio-${VERSION}-server.jar")

	// COMPILE
	mvnArgs := getMvnArgs()
	run("compiling repo", "mvn", mvnArgs...)

	// SET DESTINATION PATHS
	dstDir := fmt.Sprintf("alluxio-%s", version)
	dstPath := filepath.Join(cwd, dstDir)
	run(fmt.Sprintf("removing any existing %v", dstPath), "rm", "-rf", dstPath)
	tarball := fmt.Sprintf("alluxio-%s.tar.gz", version)
	fmt.Printf("Creating %s:\n", tarball)

	// CREATE NEEDED DIRECTORIES
	// Create the directory for the server jar.
	mkdir(filepath.Join(dstPath, "server"))
	// Create directories for the client jars.
	for _, framework := range frameworks {
		mkdir(filepath.Join(dstPath, "client", framework))
	}
	mkdir(filepath.Join(dstPath, "logs"))
	mkdir(filepath.Join(dstPath, "integration", "bin"))
	mkdir(filepath.Join(dstPath, "job", "bin"))

	// ADD ALLUXIO SERVER JAR TO DISTRIBUTION
	run("adding Alluxio server jar", "mv", fmt.Sprintf("assembly/target/alluxio-assemblies-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "server", fmt.Sprintf("alluxio-%v-server.jar", version)))

	// BUILD ALLUXIO CLIENTS JARS AND ADD THEM TO DISTRIBUTION
	chdir(filepath.Join(srcPath, "core", "client"))
	for _, framework := range frameworks {
		clientArgs := mvnArgs
		if framework != "hadoop" {
			clientArgs = append(clientArgs, fmt.Sprintf("-P%s", framework))
		}
		run(fmt.Sprintf("building Alluxio %s client jar", framework), "mvn", clientArgs...)
		run(fmt.Sprintf("adding Alluxio %s client jar", framework), "mv", fmt.Sprintf("target/alluxio-core-client-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "client", fmt.Sprintf("%v/alluxio-%v-%v-client.jar", framework, version, framework)))
	}

	// ADD / REMOVE ADDITIONAL CONTENT TO / FROM DISTRIBUTION
	chdir(srcPath)
	run("adding Alluxio scripts", "mv", "bin", "conf", "libexec", dstPath)
	run("removing bin/alluxio-fuse.sh", "rm", filepath.Join(dstPath, "bin/alluxio-fuse.sh"))
	for _, file := range []string{
		"alluxio-env-mesos.sh",
		"alluxio-master-mesos.sh",
		"alluxio-mesos.sh",
		"alluxio-worker-mesos.sh",
		"common.sh",
	} {
		path := filepath.Join("integration", "bin", file)
		run(fmt.Sprintf("adding %v", path), "mv", path, filepath.Join(dstPath, path))
	}
	for _, file := range []string{
		"alluxio-start.sh",
		"alluxio-stop.sh",
		"alluxio-workers.sh",
	} {
		path := filepath.Join("job", "bin", file)
		run(fmt.Sprintf("adding %v", path), "mv", path, filepath.Join(dstPath, path))
	}

	// CREATE DISTRIBUTION TARBALL AND CLEANUP
	chdir(cwd)
	run("creating the distribution tarball", "tar", "-czvf", tarball, dstDir)
	run("removing the repository", "rm", "-rf", srcPath, dstPath)

	return nil
}

func main() {
	if err := generateTarball(); err != nil {
		fmt.Printf("Failed to generate tarball: %v", err)
		os.Exit(1)
	}
}
