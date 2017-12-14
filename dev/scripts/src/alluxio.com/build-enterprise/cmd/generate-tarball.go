package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	annotation "github.com/TachyonNexus/golluxio/annotation/cmd"
	"v.io/x/lib/cmdline"
)

const edition = "enterprise"

var (
	cmdSingle = &cmdline.Command{
		Name:   "single",
		Short:  "Generates an enterprise tarball",
		Long:   "Generates an enterprise tarball",
		Runner: cmdline.RunnerFunc(single),
	}

	targetFlag  string
	mvnArgsFlag string

	webappDir = "core/server/common/src/main/webapp"
	webappWar = "assembly/webapp.war"
)

func init() {
	cmdSingle.Flags.StringVar(&targetFlag, "target", fmt.Sprintf("alluxio-%v.tar.gz", versionMarker),
		fmt.Sprintf("an optional target name for the generated tarball. The default is alluxio-%v.tar.gz. The string %q will be substituted with the built version. "+
			`Note that trailing ".tar.gz" will be stripped to determine the name for the Root directory of the generated tarball`, versionMarker, versionMarker))
	cmdSingle.Flags.StringVar(&mvnArgsFlag, "mvn-args", "", `a comma-separated list of additional Maven arguments to build with, e.g. -mvn-args "-Pspark,-Dhadoop.version=2.2.0"`)
}

func single(_ *cmdline.Env, _ []string) error {
	if err := updateRootFlags(); err != nil {
		return err
	}
	if err := checkRootFlags(); err != nil {
		return err
	}
	if err := generateTarball(); err != nil {
		return err
	}
	return nil
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
	args := []string{"clean", "install", "-DskipTests", "-Dfindbugs.skip", "-Dmaven.javadoc.skip", "-Dcheckstyle.skip"}
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
	pathsToCopy := []string{
		// BIN
		"bin/alluxio",
		"bin/alluxio-masters.sh",
		"bin/alluxio-mount.sh",
		"bin/alluxio-start.sh",
		"bin/alluxio-stop.sh",
		"bin/alluxio-workers.sh",
		// CLIENT
		fmt.Sprintf("client/alluxio-%v-client.jar", version),
		// CONF
		"conf/alluxio-env.sh.template",
		"conf/alluxio-site.properties.template",
		"conf/core-site.xml.template",
		"conf/log4j.properties",
		"conf/masters",
		"conf/metrics.properties.template",
		"conf/workers",
		// LIB
		fmt.Sprintf("lib/alluxio-underfs-fork-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-gcs-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-jdbc-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-local-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-oss-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-swift-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-s3a-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-wasb-%v.jar", version),
		// LIBEXEC
		"libexec/alluxio-config.sh",
		// DOCKER
		"integration/docker/bin/alluxio-master.sh",
		"integration/docker/bin/alluxio-job-master.sh",
		"integration/docker/bin/alluxio-job-worker.sh",
		"integration/docker/bin/alluxio-proxy.sh",
		"integration/docker/bin/alluxio-worker.sh",
		// KUBERNETES
		"integration/kubernetes/alluxio-journal-volume.yaml.template",
		"integration/kubernetes/alluxio-master.yaml.template",
		"integration/kubernetes/alluxio-worker.yaml.template",
		"integration/kubernetes/conf/alluxio.properties.template",
	}
	for _, path := range pathsToCopy {
		mkdir(filepath.Join(dstPath, filepath.Dir(path)))
		run(fmt.Sprintf("adding %v", path), "mv", path, filepath.Join(dstPath, path))
	}

	// Create empty directories for default UFS and Docker integration.
	mkdir(filepath.Join(dstPath, "underFSStorage"))
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
	for _, moduleName := range strings.Split(ufsModulesFlag, ",") {
		ufsModule, ok := ufsModules[moduleName]
		if !ok {
			// This should be impossible, we validate ufsModulesFlag at the start.
			fmt.Fprintf(os.Stderr, "Unrecognized ufs module: %v", moduleName)
			os.Exit(1)
		}
		ufsJar := fmt.Sprintf("alluxio-underfs-%v-%v.jar", ufsModule.name, version)
		run(fmt.Sprintf("adding ufs module %v to lib/", moduleName), "mv", filepath.Join(srcPath, "lib", ufsJar), filepath.Join(dstPath, "lib"))
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

	srcPath, err := ioutil.TempDir("", edition)
	if err != nil {
		return fmt.Errorf("Failed to create temp directory: %v", err)
	}

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("Failed to determine file of the go script")
	}
	// Relative path from this class to the root enterprise directory.
	repoPath := filepath.Join(filepath.Dir(file), "../../../../../../")
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

	// ERASE ENTERPRISE ANNOTATIONS
	fmt.Printf("  erasing enterprise annotations ... ")
	if err := annotation.Erase(srcPath); err != nil {
		return err
	}
	fmt.Println("done")

	// COMPILE
	mvnArgs := getCommonMvnArgs()
	run("compiling repo", "mvn", mvnArgs...)
	// Compile ufs modules for the main build
	for _, moduleName := range strings.Split(ufsModulesFlag, ",") {
		ufsModule := ufsModules[moduleName]
		ufsMvnArgs := mvnArgs
		for _, arg := range strings.Split(ufsModule.mavenArgs, " ") {
			ufsMvnArgs = append(ufsMvnArgs, arg)
		}
		run(fmt.Sprintf("compiling ufs module %v", moduleName), "mvn", ufsMvnArgs...)
		srcUfsJar := fmt.Sprintf("alluxio-underfs-hdfs-%v.jar", version)
		dstUfsJar := fmt.Sprintf("alluxio-underfs-%v-%v.jar", ufsModule.name, version)
		run(fmt.Sprintf("saving ufs module %v", moduleName), "mv", filepath.Join(srcPath, "lib", srcUfsJar), filepath.Join(srcPath, "lib", dstUfsJar))
	}

	// SET DESTINATION PATHS
	tarball := strings.Replace(targetFlag, versionMarker, version, 1)
	dstDir := strings.TrimSuffix(filepath.Base(tarball), ".tar.gz")
	dstPath := filepath.Join(cwd, dstDir)
	run(fmt.Sprintf("removing any existing %v", dstPath), "rm", "-rf", dstPath)
	fmt.Printf("Creating %s:\n", tarball)

	// CREATE NEEDED DIRECTORIES
	// Create the directory for the server jar.
	mkdir(filepath.Join(dstPath, "assembly"))
	// Create directories for the client jar.
	mkdir(filepath.Join(dstPath, "client"))
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
	run("building Alluxio client jar", "mvn", getCommonMvnArgs()...)
	run("adding Alluxio client jar", "mv", filepath.Join(srcPath, "client", fmt.Sprintf("alluxio-%v-client.jar", version)), filepath.Join(dstPath, "client", fmt.Sprintf("alluxio-%v-client.jar", version)))

	// CREATE DISTRIBUTION TARBALL AND CLEANUP
	chdir(cwd)
	run("creating the distribution tarball", "tar", "-czvf", tarball, dstDir)
	run("removing the repository", "rm", "-rf", srcPath, dstPath)

	return nil
}
