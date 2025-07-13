package errors

import (
	"errors"
	"fmt"
)

var (
	UnreachableCode = errors.New("this could should be unreachable")
)

type DbError struct {
	Err          error
	Reconcilable bool
}

func (d DbError) Error() string {
	if d.Reconcilable {
		return fmt.Sprintf(d.Err.Error() + " - is reconscilable")
	}

	return fmt.Sprintf(d.Err.Error() + " - is not reconscilable")
}
