package cmd

import (
	"path/filepath"
	"strings"
)

type fileType int

const (
	unknownType fileType = iota
	javaType
	markdownType
	propertiesType
	shellType
	xmlType
)

const (
	addAnnotationText      = "ENTERPRISE ADD"
	endAnnotationText      = "ENTERPRISE END"
	removeAnnotationText   = "ENTERPRISE REMOVE"
	replaceAnnotationText  = "ENTERPRISE REPLACE"
	withAnnotationText     = "ENTERPRISE WITH"
)

func inferFileType(filename string) fileType {
	// TODO: support infering file type from shebang
	switch filepath.Ext(filename) {
	case ".java":
		return javaType
	case ".md":
		return markdownType
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
	case markdownType, xmlType:
		return "<!-- "
	}
	return ""
}

func (ft fileType) endComment() string {
	switch ft {
	case markdownType, xmlType:
		return " -->"
	}
	return ""
}

func (ft fileType) addAnnotation() string {
	return ft.startComment() + addAnnotationText + ft.endComment()
}

func (ft fileType) endAnnotation() string {
	return ft.startComment() + endAnnotationText + ft.endComment()
}

func (ft fileType) removeAnnotation() string {
	return ft.startComment() + removeAnnotationText + ft.endComment()
}

func (ft fileType) replaceAnnotation() string {
	return ft.startComment() + replaceAnnotationText + ft.endComment()
}

func (ft fileType) withAnnotation() string {
	return ft.startComment() + withAnnotationText + ft.endComment()
}

func (ft fileType) String() string {
	switch ft {
	case javaType:
		return "Java file"
	case markdownType:
		return "Markdown file"
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
