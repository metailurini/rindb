package rindb

import "log"

func LOG(logType string, msg string, args []any) {
	log.Printf("["+logType+"] "+msg+"\n", args...)
}

func DEBUG(msg string, args ...any) { LOG("DEBUG", msg, args) }
func INFO(msg string, args ...any)  { LOG("INFO", msg, args) }
func WARN(msg string, args ...any)  { LOG("WARN", msg, args) }
func ERROR(msg string, args ...any) { LOG("ERROR", msg, args) }
