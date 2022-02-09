package shardkv

import (
	"fmt"
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
func cloneBytes(src []byte) []byte {
	res := make([]byte, len(src))
	copy(res, src)
	return res
}

func (kv *ShardKV) shardkvInfo() string {
	return fmt.Sprintf(" server [%v, %v] ", kv.gid, kv.me)
}

func (kv *ShardKV) shardInfo() string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shards := []int{}
	for shard := range kv.shards {
		if kv.shards_state[shard] == Valid {
			shards = append(shards, shard)
		}
	}

	return fmt.Sprintf(" valid shards: %v", shards)
}

func (kv *ShardKV) exsitConfig(cfgNum int) (shardctrler.Config, bool) {
	cfg := kv.ck.Query(cfgNum)
	return cfg, cfg.Num == cfgNum
}
