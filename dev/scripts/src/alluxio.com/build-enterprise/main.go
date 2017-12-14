package main

import (
	"v.io/x/lib/cmdline"
	"alluxio.com/build-enterprise/cmd"
)

func main() {
	cmdline.Main(cmd.Root)
}
