package shardkv

import (
	"log"

	"6.824/shardctrler"
)

const (
	Debug = false
	INFO  = false
	ERROR = false
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("[DEBUG]"+format, a...)
	}
	return
}

func Info(format string, a ...interface{}) (n int, err error) {
	if INFO {
		log.Printf("[INFO]"+format, a...)
	}
	return
}

func Error(format string, a ...interface{}) (n int, err error) {
	if ERROR {
		log.Printf("[ERROR]"+format, a...)
	}
	return
}

func showConfigInfo(cfg *shardctrler.Config) {
	DPrintf("all the shards : %v", cfg.Shards)
}

func cloneBytes(src []byte) []byte {
	res := make([]byte, len(src))
	copy(res, src)
	return res
}
