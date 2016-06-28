package cmd

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

type state int

const (
	initialState state = iota
	addState
	editState
	removeState
	replacesState
)

type stateMachine struct {
	s        state
	filename string
	line     int
}

func newStateMachine(filename string) *stateMachine {
	return &stateMachine{
		s:        initialState,
		filename: filepath.Base(filename),
		line:     0,
	}
}

func (sm *stateMachine) next(token string, line int, ft fileType) []warning {
	result := []warning{}
	switch strings.TrimSpace(token) {
	case ft.addAnnotation():
		if sm.s != initialState {
			result = append(result, sm.warning()...)
		}
		sm.s, sm.line = addState, line
	case ft.editAnnotation():
		if sm.s != initialState {
			result = append(result, sm.warning()...)
		}
		sm.s, sm.line = editState, line
	case ft.endAnnotation():
		switch sm.s {
		case editState:
			result = append(result, warning{
				filename: sm.filename,
				line:     sm.line,
				message:  fmt.Sprintf("annotation %q is not followed by annotation %q", editAnnotationText, replacesAnnotationText),
			})
		case initialState:
			result = append(result, warning{
				filename: sm.filename,
				line:     line,
				message:  fmt.Sprintf("annotation %q is not preceeded by either %q or %q", endAnnotationText, addAnnotationText, removeAnnotationText),
			})
		}
		sm.s, sm.line = initialState, line
	case ft.removeAnnotation():
		if sm.s != initialState {
			result = append(result, sm.warning()...)
		}
		sm.s, sm.line = removeState, line
	case ft.replacesAnnotation():
		if sm.s != editState {
			result = append(result, warning{
				filename: sm.filename,
				line:     line,
				message:  fmt.Sprintf("annotation %q is not preceeded by annotation %q", replacesAnnotationText, editAnnotationText),
			})
		}
		sm.s, sm.line = replacesState, line
	}
	return result
}

func (sm *stateMachine) process(token string, writer io.Writer, ft fileType) error {
	switch strings.TrimSpace(token) {
	case ft.addAnnotation(), ft.editAnnotation(), ft.endAnnotation(), ft.removeAnnotation(), ft.replacesAnnotation():
		return nil
	}
	switch sm.s {
	case initialState:
		if _, err := writer.Write([]byte(token + "\n")); err != nil {
			return err
		}
	case addState, editState:
		return nil
	case removeState, replacesState:
		if strings.TrimSpace(token) == strings.TrimSpace(ft.startComment()) {
			token = ""
		}
		// Remove start of comment
		if index := strings.Index(token, ft.startComment()); index != -1 {
			token = token[:index] + token[index+len(ft.startComment()):]
		}
		// Remove end of comment
		if index := strings.LastIndex(token, ft.endComment()); len(ft.endComment()) > 0 && index != -1 {
			token = token[:index] + token[index+len(ft.endComment()):]
		}
		if _, err := writer.Write([]byte(token + "\n")); err != nil {
			return err
		}
	}
	return nil
}

func (sm *stateMachine) warning() []warning {
	switch sm.s {
	case addState:
		return []warning{warning{
			filename: sm.filename,
			line:     sm.line,
			message:  fmt.Sprintf("annotation %q is not followed by annotation %q", addAnnotationText, endAnnotationText),
		}}
	case editState:
		return []warning{warning{
			filename: sm.filename,
			line:     sm.line,
			message:  fmt.Sprintf("annotation %q is not followed by annotation %q", editAnnotationText, replacesAnnotationText),
		}}
	case removeState:
		return []warning{warning{
			filename: sm.filename,
			line:     sm.line,
			message:  fmt.Sprintf("annotation %q is not followed by annotation %q", removeAnnotationText, endAnnotationText),
		}}
	case replacesState:
		return []warning{warning{
			filename: sm.filename,
			line:     sm.line,
			message:  fmt.Sprintf("annotation %q is not followed by annotation %q", replacesAnnotationText, endAnnotationText),
		}}
	}
	return nil
}
