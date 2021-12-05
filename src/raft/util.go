package raft

import "log"

// Debugging
const Debug = true
const INFO = true
const ERROR = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("[DEBUG] "+format, a...)
	}
	return
}

func Info(format string, a ...interface{}) (n int, err error) {
	if INFO {
		log.Printf("[INFO] "+format, a...)
	}
	return
}

func Error(format string, a ...interface{}) (n int, err error) {
	if ERROR {
		log.Printf("[ERROR] "+format, a...)
	}
	return
}
