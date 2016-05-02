package cmd

import (
	"path/filepath"
	"strings"
)

type fileType int

const (
	unknownType fileType = iota
	javaType
	propertiesType
	shellType
	xmlType
)

const (
	addAnnotationText      = "ENTERPRISE ADD"
	editAnnotationText     = "ENTERPRISE EDIT"
	endAnnotationText      = "ENTERPRISE END"
	replacesAnnotationText = "ENTERPRISE REPLACES"
)

func inferFileType(filename string) fileType {
	// TODO: support infering file type from shebang
	switch filepath.Ext(filename) {
	case ".java":
		return javaType
	case ".properties":
		return propertiesType
	case ".sh":
		return shellType
	case ".xml":
		return xmlType
	case ".template":
		return inferFileType(strings.TrimSuffix(filename, filepath.Ext(filename)))
	default:
		return unknownType
	}
}

func (ft fileType) startComment() string {
	switch ft {
	case javaType:
		return "// "
	case propertiesType, shellType:
		return "# "
	case xmlType:
		return "<!-- "
	}
	return ""
}

func (ft fileType) endComment() string {
	switch ft {
	case xmlType:
		return " -->"
	}
	return ""
}

func (ft fileType) addAnnotation() string {
	return ft.startComment() + addAnnotationText + ft.endComment()
}

func (ft fileType) editAnnotation() string {
	return ft.startComment() + editAnnotationText + ft.endComment()
}

func (ft fileType) endAnnotation() string {
	return ft.startComment() + endAnnotationText + ft.endComment()
}

func (ft fileType) replacesAnnotation() string {
	return ft.startComment() + replacesAnnotationText + ft.endComment()
}

func (ft fileType) String() string {
	switch ft {
	case javaType:
		return "Java file"
	case propertiesType:
		return "properties file"
	case shellType:
		return "shell script"
	case xmlType:
		return "XML file"
	default:
		return "unknown file type"
	}
}
