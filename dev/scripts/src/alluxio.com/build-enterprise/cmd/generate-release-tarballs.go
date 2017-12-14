package cmd

import (
	"fmt"
	"strings"

	"v.io/x/lib/cmdline"
)

var (
	cmdRelease = &cmdline.Command{
		Name:   "release",
		Short:  "Generates all enterprise release tarballs",
		Long:   "Generates all enterprise release tarballs",
		Runner: cmdline.RunnerFunc(release),
	}
)

func checkReleaseFlags() error {
	if err := checkRootFlags(); err != nil {
		return err
	}
	for _, distribution := range strings.Split(hadoopDistributionsFlag, ",") {
		_, ok := hadoopDistributions[distribution]
		if !ok {
			return fmt.Errorf("hadoop distribution %s not recognized\n", distribution)
		}
	}
	return nil
}

func release(_ *cmdline.Env, _ []string) error {
	if err := updateRootFlags(); err != nil {
		return err
	}
	if err := checkReleaseFlags(); err != nil {
		return err
	}
	if err := generateTarballs(); err != nil {
		return err
	}
	return nil
}

func generateTarballs() error {
	for _, distribution := range strings.Split(hadoopDistributionsFlag, ",") {
		version, ok := hadoopDistributions[distribution]
		if !ok {
			return fmt.Errorf("hadoop distribution %s not recognized\n", distribution)
		}
		targetFlag = fmt.Sprintf("alluxio-%v-%v.tar.gz", versionMarker, distribution)
		mvnArgs := []string{fmt.Sprintf("-Dhadoop.version=%v", version), fmt.Sprintf("-P%v", version.hadoopProfile())}
		if version.hasHadoopKMS() {
			mvnArgs = append(mvnArgs, "-Phadoop-kms")
		}
		mvnArgsFlag = strings.Join(mvnArgs, ",")
		fmt.Printf("Generating distribution for %v at %v", distribution, targetFlag)
		if err := generateTarball(); err != nil {
			return err
		}
	}
	return nil
}
