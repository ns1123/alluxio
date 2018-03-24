package cmd

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

type commentType int

const (
	unknownType commentType = iota
	poundType
	slashType
	markupType
)

const (
	addAnnotationText      = "ALLUXIO CS ADD"
	endAnnotationText      = "ALLUXIO CS END"
	modifiedAnnotationText = "ALLUXIO CS MODIFIED"
	removeAnnotationText   = "ALLUXIO CS REMOVE"
	replaceAnnotationText  = "ALLUXIO CS REPLACE"
	withAnnotationText     = "ALLUXIO CS WITH"
)

var allTypes = []commentType{
	poundType,
	slashType,
	markupType,
}

func inferCommentType(filename string) (commentType, error) {
	ct := inferCommentTypeFromName(filename)
	if ct != unknownType {
		return ct, nil
	}
	file, err := os.Open(filename)
	if err != nil {
		return unknownType, err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if ct := inferCommentTypeFromLine(line); ct != unknownType {
			return ct, nil
		}
	}
	return unknownType, nil
}

func inferCommentTypeFromLine(line string) commentType {
	for _, commentType := range allTypes {
		if line == commentType.startComment()+modifiedAnnotationText+commentType.endComment() {
			return commentType
		}
	}
	return unknownType
}

func inferCommentTypeFromName(filename string) commentType {
	// TODO: support infering file type from shebang
	switch filepath.Ext(filename) {
	case "":
		return unknownType
	case ".java", ".proto", ".thrift", ".go":
		return slashType
	case ".properties", ".sh", ".yaml":
		return poundType
	case ".jsp", ".md", ".xml":
		return markupType
	case ".template":
		return inferCommentTypeFromName(strings.TrimSuffix(filename, filepath.Ext(filename)))
	default:
		return unknownType
	}
}

func (ct commentType) startComment() string {
	switch ct {
	case slashType:
		return "// "
	case poundType:
		return "# "
	case markupType:
		return "<!-- "
	}
	return ""
}

func (ct commentType) endComment() string {
	switch ct {
	case markupType:
		return " -->"
	}
	return ""
}

func (ct commentType) addAnnotation() string {
	return ct.startComment() + addAnnotationText + ct.endComment()
}

func (ct commentType) endAnnotation() string {
	return ct.startComment() + endAnnotationText + ct.endComment()
}

func (ct commentType) modifiedAnnotation() string {
	return ct.startComment() + modifiedAnnotationText + ct.endComment()
}

func (ct commentType) removeAnnotation() string {
	return ct.startComment() + removeAnnotationText + ct.endComment()
}

func (ct commentType) replaceAnnotation() string {
	return ct.startComment() + replaceAnnotationText + ct.endComment()
}

func (ct commentType) withAnnotation() string {
	return ct.startComment() + withAnnotationText + ct.endComment()
}

func (ct commentType) String() string {
	if ct == unknownType {
		return "unknown comment type"
	}
	return ct.startComment() + ct.endComment()
}
