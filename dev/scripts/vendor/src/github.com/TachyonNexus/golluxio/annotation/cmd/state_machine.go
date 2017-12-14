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
	removeState
	replaceState
	withState
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

// next advances the state machine to the next token. This is the only method in stateMachine which mutates the state
// machine's state.
func (sm *stateMachine) next(token string, line int, ct commentType) []warning {
	result := []warning{}
	switch strings.TrimSpace(token) {
	case ct.addAnnotation():
		if sm.s != initialState {
			result = append(result, sm.warning()...)
		}
		sm.s, sm.line = addState, line
	case ct.replaceAnnotation():
		if sm.s != initialState {
			result = append(result, sm.warning()...)
		}
		sm.s, sm.line = replaceState, line
	case ct.endAnnotation():
		switch sm.s {
		case replaceState:
			result = append(result, warning{
				Filename: sm.filename,
				Line:     sm.line,
				Message:  fmt.Sprintf("annotation %q is not followed by annotation %q", replaceAnnotationText, withAnnotationText),
			})
		case initialState:
			result = append(result, warning{
				Filename: sm.filename,
				Line:     line,
				Message:  fmt.Sprintf("annotation %q is not preceeded by either %q or %q", endAnnotationText, addAnnotationText, removeAnnotationText),
			})
		}
		sm.s, sm.line = initialState, line
	case ct.removeAnnotation():
		if sm.s != initialState {
			result = append(result, sm.warning()...)
		}
		sm.s, sm.line = removeState, line
	case ct.withAnnotation():
		if sm.s != replaceState {
			result = append(result, warning{
				Filename: sm.filename,
				Line:     line,
				Message:  fmt.Sprintf("annotation %q is not preceeded by annotation %q", withAnnotationText, replaceAnnotationText),
			})
		}
		sm.s, sm.line = withState, line
	case ct.modifiedAnnotation():
		sm.line = line
	}
	return result
}

// processRevert processes the token for the purpose of reverting back to open-source code. This processing removes and
// reverts annotations, removing CS-only code within ALLUXIO CS ADD and ALLUXIO CS WITH blocks, and un-commenting
// open-source code within ALLUXIO CS REMOVE or ALLUXIO CS REPLACE blocks.
func (sm *stateMachine) processRevert(token string, writer io.Writer, ct commentType) error {
	switch strings.TrimSpace(token) {
	case ct.addAnnotation(), ct.endAnnotation(), ct.modifiedAnnotation(), ct.removeAnnotation(), ct.replaceAnnotation(), ct.withAnnotation():
		return nil
	}
	switch sm.s {
	case initialState:
		if _, err := writer.Write([]byte(token + "\n")); err != nil {
			return err
		}
	case addState, withState:
		return nil
	case removeState, replaceState:
		if strings.TrimSpace(token) == strings.TrimSpace(ct.startComment()) {
			token = ""
		}
		// Remove start of comment
		if index := strings.Index(token, ct.startComment()); index != -1 {
			token = token[:index] + token[index+len(ct.startComment()):]
		}
		// Remove end of comment
		if index := strings.LastIndex(token, ct.endComment()); len(ct.endComment()) > 0 && index != -1 {
			token = token[:index] + token[index+len(ct.endComment()):]
		}
		if _, err := writer.Write([]byte(token + "\n")); err != nil {
			return err
		}
	}
	return nil
}

// processErase processes the token for the purpose of erasing annotations. This processing removes annotations and any
// within ALLUXIO CS REMOVE or ALLUXIO CS REPLACE blocks
func (sm *stateMachine) processErase(token string, writer io.Writer, ct commentType) error {
	switch strings.TrimSpace(token) {
	case ct.addAnnotation(), ct.endAnnotation(), ct.modifiedAnnotation(), ct.removeAnnotation(), ct.replaceAnnotation(), ct.withAnnotation():
		return nil
	}
	switch sm.s {
	case initialState, addState, withState:
		if _, err := writer.Write([]byte(token + "\n")); err != nil {
			return err
		}
	case removeState, replaceState:
		return nil
	}
	return nil
}

func (sm *stateMachine) warning() []warning {
	switch sm.s {
	case addState:
		return []warning{{
			Filename: sm.filename,
			Line:     sm.line,
			Message:  fmt.Sprintf("annotation %q is not followed by annotation %q", addAnnotationText, endAnnotationText),
		}}
	case removeState:
		return []warning{{
			Filename: sm.filename,
			Line:     sm.line,
			Message:  fmt.Sprintf("annotation %q is not followed by annotation %q", removeAnnotationText, endAnnotationText),
		}}
	case replaceState:
		return []warning{{
			Filename: sm.filename,
			Line:     sm.line,
			Message:  fmt.Sprintf("annotation %q is not followed by annotation %q", replaceAnnotationText, withAnnotationText),
		}}
	case withState:
		return []warning{{
			Filename: sm.filename,
			Line:     sm.line,
			Message:  fmt.Sprintf("annotation %q is not followed by annotation %q", withAnnotationText, endAnnotationText),
		}}
	}
	return nil
}
