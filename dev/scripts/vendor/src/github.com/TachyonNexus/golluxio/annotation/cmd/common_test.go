package cmd

var revisionedPaths = []string{
	"excluded_fail.xml",
	"excluded_ok.xml",
	"no_extension_java",
	"no_extension_sh",
	"no_extension_shebang",
	"revisioned_dir/excluded_fail.xml",
	"revisioned_dir/excluded_ok.xml",
	"revisioned_dir/revisioned_fail.xml",
	"revisioned_dir/revisioned_ok.xml",
	"revisioned_fail.java",
	"revisioned_fail.java.template",
	"revisioned_fail.jsp",
	"revisioned_fail.md",
	"revisioned_fail.properties",
	"revisioned_fail.properties.template",
	"revisioned_fail.proto",
	"revisioned_fail.sh",
	"revisioned_fail.thrift",
	"revisioned_fail.xml",
	"revisioned_fail.xml.template",
	"revisioned_ok.java",
	"revisioned_ok.java.template",
	"revisioned_ok.jsp",
	"revisioned_ok.md",
	"revisioned_ok.properties",
	"revisioned_ok.properties.template",
	"revisioned_ok.proto",
	"revisioned_ok_replace_empty_line.java",
	"revisioned_ok.sh",
	"revisioned_ok.thrift",
	"revisioned_ok.xml",
	"revisioned_ok.xml.template",
	"revisioned_ok.yaml",
	"revisioned_ok.yaml.template",
}

type buffer string

func (b *buffer) Close() error {
	return nil
}

func (b *buffer) Write(p []byte) (int, error) {
	*b += buffer(p)
	return len(p), nil
}

func (b *buffer) String() string {
	return string(*b)
}
