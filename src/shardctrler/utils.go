package shardctrler

import (
	"fmt"
	"log"
	"sort"
)

const Debug = false
const ERROR = false
const INFO = false

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

func minInt(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func copyMap(dst, src map[int][]string) {
	for k, v := range src {
		dst[k] = v
	}
}

// mergeGroups merge all the maps of gid -> servers
func mergeGroups(ms ...map[int][]string) map[int][]string {
	res := make(map[int][]string)
	for _, m := range ms {
		for k, v := range m {
			res[k] = append(res[k], v...)
		}
	}
	return res
}

func removeGroups(m map[int][]string, gids []int) map[int][]string {
	res := make(map[int][]string)
	copyMap(res, m)

	for _, v := range gids {
		delete(res, v)
	}
	return res
}

func (sc *ShardCtrler) addGroupUnlocked(gid int) {
	_, ok := sc.gsmap[gid]
	if ok {
		err := fmt.Sprintf("gid %v has been mapped", gid)
		panic(err)
	}

	sc.gsid++
	sc.gsmap[gid] = sc.gsid
}

func (sc *ShardCtrler) removeGroupUnlocked(gid int) {
	_, ok := sc.gsmap[gid]
	if !ok {
		err := fmt.Sprintf("gid %v should exists.", gid)
		panic(err)
	}

	delete(sc.gsmap, gid)
}

func (sc *ShardCtrler) latestConfigUnlocked() Config {
	return sc.configs[len(sc.configs)-1]
}

// getTransferOpNum will compare two config and return the transfer operations number
func (sc *ShardCtrler) getTransferOpNum(ocfg, ncfg *Config) int {
	ops := 0
	for i := 0; i < NShards; i++ {
		if ocfg.Shards[i] != ncfg.Shards[i] {
			ops++
		}
	}
	return ops
}

// divideShardsAfterJoin will divide the shards and apply it to ncfg
// ocfg gives useful information for dividing
func (sc *ShardCtrler) divideShardsAfterJoin(ocfg, ncfg *Config) {
	// // new group won't be used
	// if len(ocfg.Groups) >= NShards {
	// 	return
	// }

	// // get all groups and sort them according to gsid
	// groups := []struct {
	// 	gid  int
	// 	gsid int
	// }{}
	// for k := range ncfg.Groups {
	// 	groups = append(groups, struct {
	// 		gid  int
	// 		gsid int
	// 	}{k, sc.gsmap[k]})
	// }

	// sort.SliceStable(groups, func(i, j int) bool {
	// 	return groups[i].gsid < groups[j].gsid
	// })

	// // calculate the shards of every server
	// shards := [NShards]int{}
	// copy(shards[:], ocfg.Shards[:])

	// oldShards := make(map[int]int) // gid -> shard_num
	// for var i int = 0; i <
	// avg := NShards / len(ncfg.Groups)
	// oavg := NShards / len(ocfg.Groups)
	// buf := NShards % len(ncfg.Groups)
	// obuf := NShards % len(ocfg.Groups)
	// for i := 0; i < len(ocfg.Groups); i++ {
	// 	on := oavg
	// 	if i < obuf {
	// 		on++
	// 	}

	// 	nn := avg
	// 	if i < buf {
	// 		nn++
	// 	}

	// }
	sc.divideSimple(ocfg, ncfg)
}

// divideSimple will divide the shards using an naive way
// must hold the sc.mu.Lock
func (sc *ShardCtrler) divideSimple(ocfg, ncfg *Config) {
	groups := []struct {
		gid  int
		gsid int
	}{}
	for k := range ncfg.Groups {
		groups = append(groups, struct {
			gid  int
			gsid int
		}{k, sc.gsmap[k]})
	}

	sort.SliceStable(groups, func(i, j int) bool {
		return groups[i].gsid < groups[j].gsid
	})

	// allocate the shards evenly
	if len(ncfg.Groups) == 0 { // no groups
		return
	}

	avg := NShards / len(ncfg.Groups)
	buf := NShards % len(ncfg.Groups)

	for i := 0; i < minInt(NShards, len(groups)); i++ {
		cur := avg
		if i < buf {
			cur++
		}

		// corner case of move
		for j := 0; j < NShards && cur > 0; j++ {
			if ncfg.Shards[j] == groups[i].gid {
				cur--
			}
		}

		for j := 0; j < NShards && cur > 0; j++ {
			if ncfg.Shards[j] == 0 {
				cur--
				ncfg.Shards[j] = groups[i].gid
			}
		}
	}

	// check if all shards are allocated
	for i := 0; i < NShards; i++ {
		if ncfg.Shards[i] == 0 {
			err := fmt.Sprintf("some shards haven't been allocated:%v", ncfg.Shards)
			panic(err)
		}
	}

	Error("new shards: %v", ncfg.Shards)
}

// divideShardsAfterLeave will divide the shards and apply it to ncfg
// ocfg gives useful information for dividing
func (sc *ShardCtrler) divideShardsAfterLeave(ocfg, ncfg *Config) {
	sc.divideSimple(ocfg, ncfg)
}
