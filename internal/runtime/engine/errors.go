package engine

import "fmt"

type Error struct {
	Code   string
	NodeID string
	Field  string
	Cause  error
}

func (err *Error) Error() string {
	if err.NodeID == "" {
		return err.Code
	}
	return fmt.Sprintf("%s at node %s", err.Code, err.NodeID)
}

func (err *Error) Unwrap() error { return err.Cause }
