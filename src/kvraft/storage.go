package kvraft

import (
	"bytes"
	"sync"

	"6.824/labgob"
)

type KVMapStore struct {
	mapstore map[string]string
	mu       sync.Mutex
}

func (db *KVMapStore) Get(key string) string {
	db.mu.Lock()
	defer db.mu.Unlock()
	if value, ok := db.mapstore[key]; ok {
		return value
	}

	return ""
}

func (db *KVMapStore) Put(key, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.mapstore[key] = value
}

func (db *KVMapStore) Append(key, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.mapstore[key] = db.mapstore[key] + value
}

func (db *KVMapStore) Data() []byte {
	db.mu.Lock()
	defer db.mu.Unlock()
	snapshotbuffer := new(bytes.Buffer)
	senc := labgob.NewEncoder(snapshotbuffer)
	err := senc.Encode(db.mapstore)
	if err != nil {
		panic(err)
	}
	return snapshotbuffer.Bytes()
}

func (db *KVMapStore) Load(data []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if data == nil || len(data) < 1 {
		db.mapstore = make(map[string]string)
	} else {
		buffer := bytes.NewBuffer(data)
		var store map[string]string
		sdec := labgob.NewDecoder(buffer)
		err := sdec.Decode(&store)
		if err != nil {
			panic(err)
			// panic("Decode store error")
		}

		db.mapstore = store
	}
}

func MakeMapStore() *KVMapStore {
	kvm := &KVMapStore{}
	kvm.mapstore = make(map[string]string)
	return kvm
}
