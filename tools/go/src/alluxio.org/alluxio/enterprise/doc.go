//The enteprise tool provides functionality for manipulating enterprise version fo the Alluxio open source. For instance, the tool can be used for linting the enteprise source code annotations or for reverting enterprise-only changes.
//
//Usage:
//   enterprise [flags] <command>
//
//The enterprise commands are:
//   lint        Checks validity of enterprise annotations
//   revert      Reverts enterprise-only changes of the open source code base
//   help        Display help for commands or topics
//
//The enterprise flags are:
// -repo=
//   local path to the enterprise repository
//
//The global flags are:
// -metadata=<just specify -metadata to activate>
//   Displays metadata for the program and exits.
// -time=false
//   Dump timing information to stderr before exiting the program.
//
//Enterprise lint - Checks validity of enterprise annotations
//
//This command checks validity of enterprise annotations.
//
//Usage:
//   enterprise lint [flags]
//
//The enterprise lint flags are:
// -fail-on-warning=false
//   whether to return non-zero status when warnings are encountered
//
// -repo=
//   local path to the enterprise repository
//
//Enterprise revert - Reverts enterprise-only changes of the open source code base
//
//This command reverts enterprise-only changes of the open source code base.
//
//Usage:
//   enterprise revert [flags]
//
//The enterprise revert flags are:
// -repo=
//   local path to the enterprise repository
//
//Enterprise help - Display help for commands or topics
//
//Help with no args displays the usage of the parent command.
//
//Help with args displays the usage of the specified sub-command or help topic.
//
//"help ..." recursively displays help for all commands and topics.
//
//Usage:
//   enterprise help [flags] [command/topic ...]
//
//[command/topic ...] optionally identifies a specific sub-command or help topic.
//
//The enterprise help flags are:
// -style=compact
//   The formatting style for help output:
//      compact   - Good for compact cmdline output.
//      full      - Good for cmdline output, shows all global flags.
//      godoc     - Good for godoc processing.
//      shortonly - Only output short description.
//   Override the default by setting the CMDLINE_STYLE environment variable.
// -width=<terminal width>
//   Format output to this target width in runes, or unlimited if width < 0. Defaults to the terminal width if available.  Override the default by setting the CMDLINE_WIDTH environment variable.

package main
