package main

import (
	"v.io/x/lib/cmdline"

	"alluxio.com/golluxio/annotation/cmd"
)

func main() {
	cmdline.Main(cmd.Root)
}
