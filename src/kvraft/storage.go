package kvraft

import "sync"

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
	return nil
}

func MakeMapStore() *KVMapStore {
	kvm := &KVMapStore{}
	kvm.mapstore = make(map[string]string)
	return kvm
}
