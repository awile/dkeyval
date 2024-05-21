package main

type DB struct {
	kvStore *KeyValue
	wal     *WAL
}

func (db *DB) Set(key string, value interface{}) {
	db.wal.AppendToWAL(key, value, false)
	db.kvStore.Set(key, value)
}

func (db *DB) Get(key string) interface{} {
	return db.kvStore.Get(key)
}

func (db *DB) Delete(key string) {
	db.wal.AppendToWAL(key, nil, true)
	db.kvStore.Delete(key)
}

func NewDB() *DB {
	wal := NewWAL()
	store := wal.LoadStoreFromWAL()
	kv := NewKeyValue(store)
	return &DB{kvStore: kv, wal: wal}
}
