package main

import (
	"v.io/x/lib/cmdline"

	"alluxio.org/alluxio/enterprise/cmd"
)

func main() {
	cmdline.Main(cmd.Root)
}
