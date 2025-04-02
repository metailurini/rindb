package rindb

import (
	"flag"
	"log"
)

// isRunningTests checks if the code is being run under 'go test'.
func isRunningTests() bool {
	return flag.Lookup("test.v") != nil
}

func LOG(logType string, msg string, args []any) {
	log.Printf("["+logType+"] "+msg+"\n", args...)
}

func DEBUG(msg string, args ...any) {
	if isRunningTests() {
		LOG("DEBUG", msg, args)
	}
}
func INFO(msg string, args ...any)  { LOG("INFO", msg, args) }
func WARN(msg string, args ...any)  { LOG("WARN", msg, args) }
func ERROR(msg string, args ...any) { LOG("ERROR", msg, args) }
