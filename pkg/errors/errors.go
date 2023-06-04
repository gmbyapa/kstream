package errors

import (
	"errors"
	"fmt"
	"runtime"
)

// New creates a new instance of the base error.
func New(msg string) error {
	return fmt.Errorf("%s %s ", msg, filePath(2))
}

// NewWithFrameSkip creates a new instance of the base error and allow the user to configure skipFrames.
// useful when working with error helper functions.
func NewWithFrameSkip(msg string, skipFrames int) error {
	return fmt.Errorf("%s %s ", msg, filePath(skipFrames))
}

func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf(format+" %s", append(a, filePath(2))...)
}

// Wrap creates a new error by wrapping an existing error.
func Wrap(err error, msg string) error {
	return fmt.Errorf("%s %s \ncaused by: %w ", msg, filePath(2), err)
}

func Wrapf(err error, msg string, a ...interface{}) error {
	return fmt.Errorf("%s %s \ncaused by: %w ", fmt.Sprintf(msg, a...), filePath(2), err)
}

func UnWrapRecursivelyUntil(err error, asserter func(unWrapped error) bool) error {
	if err == nil {
		return nil
	}

	unWrapped := errors.Unwrap(err)
	if asserter(unWrapped) {
		return unWrapped
	}

	return UnWrapRecursivelyUntil(unWrapped, asserter)
}

// WrapWithFrameSkip creates a new error by wrapping an existing error.
func WrapWithFrameSkip(err error, msg string, skipFrames int) error {
	return fmt.Errorf("%s %s \ncaused by: %w ", msg, filePath(skipFrames), err)
}

// Is reports whether any error in err chain matches target.
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// filePath returns the location in which the error occurred.
func filePath(frameSkip int) string {
	pc, f, l, ok := runtime.Caller(frameSkip) // nolint
	fn := `unknown`
	if ok {
		fn = runtime.FuncForPC(pc).Name()
	}

	return fmt.Sprintf("at %s\n\t%s:%d", fn, f, l)
}
