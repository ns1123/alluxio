package cmd

import (
	"fmt"
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

	proxyURLFlag   string
	ufsModulesFlag string
	// ALLUXIO CS END
)

// ALLUXIO CS ADD
func updateRootFlags() error {
	if strings.ToLower(ufsModulesFlag) == "all" {
		ufsModulesFlag = strings.Join(validUfsModules(), ",")
	}
	return nil
}

func checkRootFlags() error {
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
	Root.Flags.StringVar(&ufsModulesFlag, "ufs-modules", strings.Join(defaultUfsModules(), ","),
		fmt.Sprintf("a comma-separated list of ufs modules to compile into the distribution tarball(s). Specify 'all' to build all ufs modules. Supported ufs modules: [%v]", strings.Join(validUfsModules(), ",")))
	// ALLUXIO CS END
}
