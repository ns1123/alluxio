// The annotation tool provides functionality for manipulating closed source annotations of the Alluxio open source. For instance, the tool can be used for linting the closed source code annotations or for reverting closed source changes.
//
// Usage:
//    annotation [flags] <command>
//
// The annotation commands are:
//    lint        Checks validity of closed source annotations
//    revert      Reverts closed source changes of the open source code base
//    help        Display help for commands or topics
//
// The annotation flags are:
//  -repo=
//    local path to the closed source repository
//
// The global flags are:
//  -metadata=<just specify -metadata to activate>
//    Displays metadata for the program and exits.
//  -time=false
//    Dump timing information to stderr before exiting the program.
//
// Annotation lint - Checks validity of closed source annotations
//
// Checks validity of closed source annotations.
//
// Usage:
//    annotation lint [flags]
//
// The annotation lint flags are:
//  -fail-on-warning=false
//    whether to return non-zero status when warnings are encountered
//
//  -repo=
//    local path to the closed source repository
//
// Annotation revert - Reverts closed source changes of the open source code base
//
// Reverts closed source changes of the open source code base.
//
// Usage:
//    annotation revert [flags]
//
// The annotation revert flags are:
//  -repo=
//    local path to the closed source repository
//
// Annotation help - Display help for commands or topics
//
// Help with no args displays the usage of the parent command.
//
// Help with args displays the usage of the specified sub-command or help topic.
//
// "help ..." recursively displays help for all commands and topics.
//
// Usage:
//    annotation help [flags] [command/topic ...]
//
// [command/topic ...] optionally identifies a specific sub-command or help topic.
//
// The annotation help flags are:
//  -style=compact
//    The formatting style for help output:
//       compact   - Good for compact cmdline output.
//       full      - Good for cmdline output, shows all global flags.
//       godoc     - Good for godoc processing.
//       shortonly - Only output short description.
//    Override the default by setting the CMDLINE_STYLE environment variable.
//  -width=<terminal width>
//    Format output to this target width in runes, or unlimited if width < 0. Defaults to the terminal width if available.  Override the default by setting the CMDLINE_WIDTH environment variable.
package main
