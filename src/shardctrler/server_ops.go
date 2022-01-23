package shardctrler

import (
	"fmt"
)

type GSPair struct {
	Gid   int
	Shard int
}

func (sc *ShardCtrler) registerMsgListener(index int, fn KVRPCHandler) bool {
	if _, ok := sc.sub[index]; ok {
		Error("chan has been registered")
		return false
	}

	sc.sub[index] = fn
	return true
}

func (sc *ShardCtrler) cancelMsgListener(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, ok := sc.sub[index]; !ok {
		return
	}

	delete(sc.sub, index)
}

// isExecuted return true if op with sn has been executed before
func (sc *ShardCtrler) isExecuted(id string, sn int64) (bool, Response) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	v, exist := sc.clientMap[id]
	if !exist {
		return false, Response{}
	}

	if v.SerialNumber < sn {
		return false, Response{}
	} else if v.SerialNumber == sn {
		return true, v
	} else {
		return true, Response{} // for any before requests, just response anything
		// client must have handled the correct reponse before
	}
}

// memorizeOp latest op of client id
// must hold sc.mu.Lock
func (sc *ShardCtrler) memorizeOpUnlocked(id string, sn int64, r Response) {
	sc.clientMap[id] = r
}

// execOp executes op in statemachine
// return the result(if not nil)
func (sc *ShardCtrler) execOp(op Op) interface{} {
	// duplicate detection
	ok, v := sc.isExecuted(op.Id, op.SerialNumber)
	if ok {
		return v.Value
	}

	// memorize and execute op atomtically
	sc.mu.Lock()
	defer sc.mu.Unlock()
	switch op.OpType {
	case JOIN:
		sc.memorizeOpUnlocked(op.Id, op.SerialNumber, Response{op.SerialNumber, nil, op.OpType})
		sc.execJoinUnlocked(&op)
		return nil
	case LEAVE:
		sc.memorizeOpUnlocked(op.Id, op.SerialNumber, Response{op.SerialNumber, nil, op.OpType})
		sc.execLeaveUnlocked(&op)
		return nil
	case MOVE:
		sc.memorizeOpUnlocked(op.Id, op.SerialNumber, Response{op.SerialNumber, nil, op.OpType})
		sc.execMoveUnlocked(&op)
		return nil
	case QUERY:
		v := sc.execQueryUnlocked(&op)
		sc.memorizeOpUnlocked(op.Id, op.SerialNumber, Response{op.SerialNumber, v, op.OpType})
		return v
	default:
		err := fmt.Sprintf("unkonwn op:%v", op)
		panic(err)
	}
}

// execJoinUnlocked executes the JOIN operation without lock
func (sc *ShardCtrler) execJoinUnlocked(op *Op) {
	ng := op.Args.(map[int][]string)

	sc.configNum++
	cfg := sc.latestConfigUnlocked()
	config := Config{}
	config.Num = sc.configNum
	for k := range ng { // add all the groups
		sc.addGroupUnlocked(k)
	}
	config.Groups = mergeGroups(cfg.Groups, ng)

	// divide the shards
	sc.divideShardsAfterJoin(&cfg, &config)
	sc.configs = append(sc.configs, config)
}

// execLeaveUnlocked executes the LEAVE operation without lock
func (sc *ShardCtrler) execLeaveUnlocked(op *Op) {
	gids := op.Args.([]int)

	sc.configNum++
	cfg := sc.latestConfigUnlocked()
	config := Config{}
	config.Num = sc.configNum
	for _, k := range gids {
		sc.removeGroupUnlocked(k)
	}
	config.Groups = removeGroups(cfg.Groups, gids)

	// divide the shards
	sc.divideShardsAfterLeave(&cfg, &config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) execMoveUnlocked(op *Op) {
	gidshard := op.Args.(GSPair)

	sc.configNum++
	cfg := sc.latestConfigUnlocked()
	config := Config{}
	config.Num = sc.configNum
	for i := 0; i < NShards; i++ {
		config.Shards[i] = 0
	}
	config.Shards[gidshard.Shard] = gidshard.Gid
	Error("move shard %v to gid %v", gidshard.Shard, gidshard.Gid)
	config.Groups = make(map[int][]string)
	copyMap(config.Groups, cfg.Groups)
	sc.divideShardsAfterJoin(&cfg, &config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) execQueryUnlocked(op *Op) Config {
	cfg := Config{}
	num := op.Args.(int)
	var goalCfg Config
	Error("current config num: %v, target config num :%v", len(sc.configs), num)
	if num == -1 || num >= sc.configNum {
		goalCfg = sc.latestConfigUnlocked()
	} else {
		goalCfg = sc.configs[num]
	}

	cfg.Groups = make(map[int][]string)
	copyMap(cfg.Groups, goalCfg.Groups)
	cfg.Num = goalCfg.Num
	cfg.Shards = [NShards]int{}
	for i := 0; i < NShards; i++ {
		cfg.Shards[i] = goalCfg.Shards[i]
	}

	Error("query shards: %v", cfg.Shards)

	return cfg
}
