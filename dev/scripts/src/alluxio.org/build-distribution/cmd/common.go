/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
)

const versionMarker = "${VERSION}"

// hadoopDistributions maps hadoop distributions to versions
var hadoopDistributions = map[string]version{
	"hadoop-1.0": parseVersion("1.0.4"),
	"hadoop-1.2": parseVersion("1.2.1"),
	"hadoop-2.2": parseVersion("2.2.0"),
	"hadoop-2.3": parseVersion("2.3.0"),
	"hadoop-2.4": parseVersion("2.4.1"),
	"hadoop-2.5": parseVersion("2.5.2"),
	"hadoop-2.6": parseVersion("2.6.5"),
	"hadoop-2.7": parseVersion("2.7.3"),
	"hadoop-2.8": parseVersion("2.8.0"),
	"hadoop-2.9": parseVersion("2.9.0"),
	// ALLUXIO CS ADD
	"cdh-4.1":  parseVersion("2.0.0-mr1-cdh4.1.2"),
	"cdh-5.4":  parseVersion("2.6.0-cdh5.4.9"),
	"cdh-5.6":  parseVersion("2.6.0-cdh5.6.1"),
	"cdh-5.8":  parseVersion("2.6.0-cdh5.8.5"),
	"cdh-5.11": parseVersion("2.6.0-cdh5.11.2"),
	"cdh-5.12": parseVersion("2.6.0-cdh5.12.2"),
	"cdh-5.13": parseVersion("2.6.0-cdh5.13.2"),
	"cdh-5.14": parseVersion("2.6.0-cdh5.14.0"),
	"hdp-2.0":  parseVersion("2.2.0.2.0.6.3-7"),
	"hdp-2.1":  parseVersion("2.4.0.2.1.7.4-3"),
	"hdp-2.2":  parseVersion("2.6.0.2.2.9.18-1"),
	"hdp-2.3":  parseVersion("2.7.1.2.3.99.0-195"),
	"hdp-2.4":  parseVersion("2.7.1.2.4.4.1-9"),
	"hdp-2.5":  parseVersion("2.7.3.2.5.5.5-2"),
	"hdp-2.6":  parseVersion("2.7.3.2.6.1.0-129"),
	"mapr-4.1": parseVersion("2.5.1-mapr-1503"),
	"mapr-5.0": parseVersion("2.7.0-mapr-1506"),
	"mapr-5.1": parseVersion("2.7.0-mapr-1602"),
	"mapr-5.2": parseVersion("2.7.0-mapr-1607"),
	// ALLUXIO CS END
	// ALLUXIO CS REMOVE
	// // This distribution type is built with 2.2.0, but doesn't include the hadoop version in the name.
	// "default": parseVersion("2.2.0"),
	// ALLUXIO CS END
}

// ALLUXIO CS ADD
type module struct {
	name      string // the name used in the generated tarball
	isDefault bool   // whether to build the module by default
	mavenArgs string // maven args for building the module
}

// ufsModules is a map from ufs module to information for building the module.
var ufsModules = map[string]module{
	"ufs-hadoop-1.0": {"hadoop-1.0", false, "-pl underfs/hdfs -Pufs-hadoop-1 -Dufs.hadoop.version=1.0.4"},
	"ufs-hadoop-1.2": {"hadoop-1.2", true, "-pl underfs/hdfs -Pufs-hadoop-1 -Dufs.hadoop.version=1.2.1"},
	"ufs-hadoop-2.2": {"hadoop-2.2", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.2.0"},
	"ufs-hadoop-2.3": {"hadoop-2.3", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.3.0"},
	"ufs-hadoop-2.4": {"hadoop-2.4", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.4.1"},
	"ufs-hadoop-2.5": {"hadoop-2.5", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.5.2"},
	"ufs-hadoop-2.6": {"hadoop-2.6", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.6.5"},
	"ufs-hadoop-2.7": {"hadoop-2.7", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.7.3"},
	"ufs-hadoop-2.8": {"hadoop-2.8", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.8.0"},
	"ufs-cdh-5.6":    {"cdh-5.6", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.6.0-cdh5.6.1"},
	"ufs-cdh-5.8":    {"cdh-5.8", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.6.0-cdh5.8.5"},
	"ufs-cdh-5.11":   {"cdh-5.11", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.6.0-cdh5.11.2"},
	"ufs-cdh-5.12":   {"cdh-5.12", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.6.0-cdh5.12.2"},
	"ufs-cdh-5.13":   {"cdh-5.13", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.6.0-cdh5.13.2"},
	"ufs-cdh-5.14":   {"cdh-5.14", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.6.0-cdh5.14.0"},
	"ufs-hdp-2.4":    {"hdp-2.4", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.7.1.2.4.4.1-9"},
	"ufs-hdp-2.5":    {"hdp-2.5", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.7.3.2.5.5.5-2"},
	"ufs-hdp-2.6":    {"hdp-2.6", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.7.3.2.6.1.0-129"},
	"ufs-mapr-4.1":   {"mapr-4.1", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.5.1-mapr-1503"},
	"ufs-mapr-5.0":   {"mapr-5.0", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.7.0-mapr-1506"},
	"ufs-mapr-5.1":   {"mapr-5.1", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.7.0-mapr-1602"},
	"ufs-mapr-5.2":   {"mapr-5.2", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.7.0-mapr-1607"},
}

// authModules is a map from authorization module to information for building the module.
var authModules = map[string]module{
	"auth-ranger-hdp-2.6": {"ranger-0.7-hdp-2.6", true, "-pl integration/authorization/hdfs -Pauth-ranger -Dauth.hadoop.version=2.7.3.2.6.1.0-129 -Dauth.plugin.name=ranger-hdp-2.6 -Dauth.plugin.version=0.7.0.2.6.1.0-129"},
	"auth-ranger-hdp-2.5": {"ranger-0.6-hdp-2.5", true, "-pl integration/authorization/hdfs -Pauth-ranger -Dauth.hadoop.version=2.7.3.2.5.5.5-2 -Dauth.plugin.name=ranger-hdp-2.5 -Dauth.plugin.version=0.6.0.2.5.5.5-2"},
	"auth-ranger-hdp-2.4": {"ranger-0.5-hdp-2.4", false, "-pl integration/authorization/hdfs -Pauth-ranger -Dauth.hadoop.version=2.7.1.2.4.4.1-9 -Dauth.plugin.name=ranger-hdp-2.4 -Dauth.plugin.version=0.5.0.2.4.4.1-9"},
}

func validModules(modules map[string]module) []string {
	result := []string{}
	for moduleName := range modules {
		result = append(result, moduleName)
	}
	sort.Strings(result)
	return result
}

func defaultModules(modules map[string]module) []string {
	result := []string{}
	for moduleName := range modules {
		if modules[moduleName].isDefault {
			result = append(result, moduleName)
		}
	}
	sort.Strings(result)
	return result
}

// ALLUXIO CS END
func validHadoopDistributions() []string {
	var result []string
	for distribution := range hadoopDistributions {
		result = append(result, distribution)
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
	stdout := &bytes.Buffer{}
	if debugFlag {
		// Stream the cmd's output (stdout and stderr) to os.Stdout, so that users can see the output while cmd is running.
		stdoutR, stdoutW := io.Pipe()
		stderrR, stderrW := io.Pipe()
		c.Stdout = stdoutW
		c.Stderr = stderrW
		stdouts := io.MultiWriter(stdout, os.Stdout)
		go func() {
			io.Copy(stdouts, stdoutR)
		}()
		go func() {
			io.Copy(os.Stderr, stderrR)
		}()
		if c.Run() != nil {
			os.Exit(1)
		}
	} else {
		c.Stdout = stdout
		stderr := &bytes.Buffer{}
		c.Stderr = stderr
		if err := c.Run(); err != nil {
			fmt.Printf("\"%v %v\" failed: %v\nstderr: <%v>\nstdout: <%v>\n", cmd, strings.Join(args, " "), err, stderr.String(), stdout.String())
			os.Exit(1)
		}
	}
	fmt.Println("done")
	return stdout.String()
}
