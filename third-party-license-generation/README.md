# Generating the License Report
From the project root directory, run:

```bash
mvn -Pthird-party-license-report license:aggregate-add-third-party
```

The final report will be generated in the parent directory under
target/generated-sources/license/THIRD-PARTY.txt.

# Detect Missing Licenses
Inspect the final (parent pom, the other modules are expected to have missing licenses) console
output for warnings such as:

```
[WARNING] There is 1 dependencies with no license :
[WARNING]  - asm--asm--3.1
```

# Add Missing Licenses
Manually determine the license type of the dependencies with no license and add a line to
missing-licenses.txt.

# Formatting the License Report
The license report is formatted in JSON by the template file json-license-format.ftl. Modify the
template file to change the format of the final output.