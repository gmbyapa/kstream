package async

import (
	"github.com/tryfix/log"
	"runtime/debug"
)

func LogPanicTrace(logger log.Logger) {
	if r := recover(); r != nil {
		logger.Fatal(r, string(debug.Stack()))
	}
}
