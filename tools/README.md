Tools
=====

This directory contains tools for working with the Alluxio Enterprise code base.

## Annotations

The following conventions are used to identify enterprise-only changes of the open source code base:

* new directories / files (preferred):
  * the parent directory contains an `.enterprise` file that enumerates all directories / files that exist only in the enterprise version of the open source code base
* adding lines to existing files (less preferred):
  * a line containing only whitespace and the comment `<START_COMMENT>ENTERPRISE ADD<END_COMMENT>` delimits the beginning of a block added in the enterprise version of the open source code base
  * a line containing only whitespace and the comment `<START_COMMENT>ENTERPRISE END<END_COMMENT>` delimits the end of the above code block
  * Example:
```
  // ENTERPRISE ADD
  awesomeEnterpriseMethod();
  // ENTERPRISE END
```
* removing lines of existing files (less preferred):
  * a line containing only whitespace and the comment `<START_COMMENT>ENTERPRISE REMOVE<END_COMMENT>` delimits the beginning of a block removed in the enterprise version of the open source code base, which should have all of its lines wrapped in `<START_COMMENT>` and `<END_COMMENT>`
  * a line containing only whitespace and the comment `<START_COMMENT>ENTERPRISE END<END_COMMENT>` delimits the end of the above code block
  * Example:
```
  // ENTERPRISE REMOVE
  // uselessOpenSourceMethod();
  // ENTERPRISE END
```
* modifying lines of existing files (least preferred):
  * a line containing only whitespace and the comment `<START_COMMENT>ENTERPRISE EDIT<END_COMMENT>` delimits the beginning of a code block in the enterprise version of the open source code base that replaces a code block in the original open source code base
  * a line containing only whitespace and the comment `<START_COMMENT>ENTERPRISE REPLACES<END_COMMENT>` delimits the end of the above code block and the beginning of the original code block, which should have all of its lines wrapped in `<START_COMMENT>` and `<END_COMMENT>`
  * a line containing only whitespace and the comment `<START_COMMENT>ENTERPRISE END<END_COMMENT>` delimits the end of the above code block
  * Example:
```
  # ENTERPRISE EDIT
  alluxio.user.file.writetype.default=ASYNC_PERSIST
  # ENTERPRISE REPLACES
  # alluxio.user.file.writetype.default=MUST_CACHE
  # ENTERPRISE END
```

White space before and after the above annotations will be ignored, but it is required that each annotation is on a separate line. The `<START_COMMENT>` and `<END_COMMENT>` strings are language specific:

* Java:
  * `<START_COMMENT>` = `"// "`
  * `<END_COMMENT>` = `""`
* properties file:
  * `<START_COMMENT>` = `"# "`
  * `<END_COMMENT>` = `""`
* XML:
  * `<START_COMMENT>` = `"<!-- "`
  * `<END_COMMENT>` = `" -->"`

## Tool `enterprise`

The `enterprise` tool provides functionality for manipulating enterprise version fo the Alluxio open source. For instance, the tool can be used for linting the enterprise source code annotations or for reverting enterprise-only changes.

To build it, run: `cd tools; make enterprise`

To learn about its functionality, run: `./tools/go/bin/enterprise help ...`

In particular, the `lint` command can be used to check enterprise annotations and the `revert` command can be used to revert enterprise annotations.
