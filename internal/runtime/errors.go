package runtime

import "fmt"

var (
	ErrInvalidRun           = fmt.Errorf("invalid workflow run")
	ErrInvalidTransition    = fmt.Errorf("invalid state transition")
	ErrTerminationFinal     = fmt.Errorf("termination intent is already final")
	ErrControlAttempt       = fmt.Errorf("control nodes cannot create attempts")
	ErrActivatedNodeSkipped = fmt.Errorf("an activated node cannot be skipped")
)

type TransitionError struct {
	Entity string
	From   string
	To     string
}

func (err *TransitionError) Error() string {
	return fmt.Sprintf("%s cannot transition from %s to %s", err.Entity, err.From, err.To)
}

func (err *TransitionError) Unwrap() error { return ErrInvalidTransition }
