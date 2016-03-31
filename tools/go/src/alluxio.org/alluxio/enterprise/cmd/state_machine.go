package cmd

import (
	"fmt"
	"path/filepath"
)

type state int

const (
	initialState state = iota
	addState
	editState
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
	switch token {
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
		if sm.s != addState && sm.s != replacesState {
			result = append(result, warning{
				filename: sm.filename,
				line:     line,
				message:  fmt.Sprintf("annotation %q is not preceeded by either %q or %q", endAnnotationText, addAnnotationText, replacesAnnotationText),
			})
		}
		sm.s, sm.line = initialState, line
	case ft.replacesAnnotation():
		if sm.s != editState {
			result = append(result, warning{
				filename: sm.filename,
				line:     line,
				message:  fmt.Sprintf("annotation %q is not preceeded by %q", replacesAnnotationText, editAnnotationText),
			})
		}
		sm.s, sm.line = replacesState, line
	}
	return result
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
	case replacesState:
		return []warning{warning{
			filename: sm.filename,
			line:     sm.line,
			message:  fmt.Sprintf("annotation %q is not followed by annotation %q", replacesAnnotationText, endAnnotationText),
		}}
	}
	return nil
}
