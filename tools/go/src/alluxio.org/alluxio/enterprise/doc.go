// The enteprise tool provides functionality for manipulating enterprise version fo the Alluxio open source. For instance, the tool can be used for linting the enteprise source code annotations or for reverting enterprise-only changes.
//
// Usage:
//    enterprise [flags] <command>
//
// The enterprise commands are:
//    lint        Checks validity of enterprise annotations
//    revert      Reverts enterprise-only changes of the open source code base
//    help        Display help for commands or topics
// Run "enterprise help [command]" for command usage.
//
// The enterprise flags are:
// -repo=
//    local path to the enterprise repository
//
// The global flags are:
// -metadata=<just specify -metadata to activate>
//    Displays metadata for the program and exits.
// -time=false
//    Dump timing information to stderr before exiting the program.

package main
