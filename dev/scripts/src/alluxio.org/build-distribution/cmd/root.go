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
	// ALLUXIO CS ADD
	"fmt"
	// ALLUXIO CS END
	"v.io/x/lib/cmdline"
	// ALLUXIO CS ADD
	"strings"
	// ALLUXIO CS END
)

var (
	Root = &cmdline.Command{
		Name:  "generate-tarballs",
		Short: "tool for creating alluxio tarballs",
		Long: `
	The publish tool contains functionality for generating either a single alluxio tarball,
or generating a suite of release tarballs.
	`,
		Children: []*cmdline.Command{
			cmdSingle,
			cmdRelease,
		},
	}

	debugFlag bool
	// ALLUXIO CS ADD
	callHomeFlag       bool
	callHomeBucketFlag string

	licenseCheckFlag     bool
	licenseSecretKeyFlag string

	nativeFlag bool

	proxyURLFlag    string
	ufsModulesFlag  string
	authModulesFlag string
	// ALLUXIO CS END
)

// ALLUXIO CS ADD
func updateRootFlags() error {
	if strings.ToLower(authModulesFlag) == "all" {
		authModulesFlag = strings.Join(validModules(authModules), ",")
	}
	if strings.ToLower(ufsModulesFlag) == "all" {
		ufsModulesFlag = strings.Join(validModules(ufsModules), ",")
	}
	return nil
}

func checkRootFlags() error {
	for _, module := range strings.Split(authModulesFlag, ",") {
		if _, ok := authModules[module]; !ok {
			return fmt.Errorf("auth module %v not recognized", module)
		}
	}
	for _, module := range strings.Split(ufsModulesFlag, ",") {
		if _, ok := ufsModules[module]; !ok {
			return fmt.Errorf("ufs module %v not recognized", module)
		}
	}
	return nil
}

// ALLUXIO CS END
func init() {
	Root.Flags.BoolVar(&debugFlag, "debug", false, "whether to run this tool in debug mode to generate additional console output")
	// ALLUXIO CS ADD
	// Call home
	Root.Flags.BoolVar(&callHomeFlag, "call-home", false, "whether the generated distributions should perform call home")
	Root.Flags.StringVar(&callHomeBucketFlag, "call-home-bucket", "", "the S3 bucket the generated distribution should upload call home information to")

	// License
	Root.Flags.BoolVar(&licenseCheckFlag, "license-check", false, "whether the generated distribution should perform license checks")
	Root.Flags.StringVar(&licenseSecretKeyFlag, "license-secret-key", "", "the cryptographic key to use for license checks. Only applicable when using license-check")

	Root.Flags.BoolVar(&nativeFlag, "native", false, "whether to build the native Alluxio libraries. See core/client/fs/src/main/native/README.md for details.")

	Root.Flags.StringVar(&proxyURLFlag, "proxy-url", "", "the URL used for communicating with company backend")
	Root.Flags.StringVar(&ufsModulesFlag, "ufs-modules", strings.Join(defaultModules(ufsModules), ","),
		fmt.Sprintf("a comma-separated list of ufs modules to compile into the distribution tarball(s). Specify 'all' to build all ufs modules. Supported ufs modules: [%v]", strings.Join(validModules(ufsModules), ",")))
	Root.Flags.StringVar(&authModulesFlag, "auth-modules", strings.Join(defaultModules(authModules), ","),
		fmt.Sprintf("a comma-separated list of authorization modules to compile into the distribution tarball(s). Specify 'all' to build all authorization modules. Supported authorization modules: [%v]", strings.Join(validModules(authModules), ",")))
	// ALLUXIO CS END
}
