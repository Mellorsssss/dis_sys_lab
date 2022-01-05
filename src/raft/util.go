package raft

import "log"

// Debugging

const Debug = false
const INFO = false
const ERROR = false
const PROFILE = true

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

func Profile(format string, a ...interface{}) (n int, err error) {
	if PROFILE {
		log.Printf("[PROFILE] "+format, a...)
	}
	return
}

func MaxInt(a int, b int) int {
	if a > b {
		return a
	}

	return b
}

func MinInt(a int, b int) int {
	if a < b {
		return a
	}

	return b
}
