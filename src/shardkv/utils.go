package shardkv

import (
	"fmt"
	"log"
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
func cloneBytes(src []byte) []byte {
	res := make([]byte, len(src))
	copy(res, src)
	return res
}

func (kv *ShardKV) shardkvInfo() string {
	return fmt.Sprintf(" server [%v, %v] ", kv.gid, kv.me)
}
