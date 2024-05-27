package main

type DB struct {
	kvStore *KeyValue
	wal     *WAL
}

func (db *DB) Set(key string, value interface{}) {
	db.wal.AppendToWAL(SegmentEntry{Key: key, Value: value, IsDeleted: false})
	db.kvStore.Set(key, value)
}

func (db *DB) Get(key string) interface{} {
	return db.kvStore.Get(key)
}

func (db *DB) Delete(key string) {
	db.wal.AppendToWAL(SegmentEntry{Key: key, Value: nil, IsDeleted: true})
	db.kvStore.Delete(key)
}

func (db *DB) updateStoreWithSegment(segment *Segment, store *KeyValue) {
	segmentEntries := segment.GetData()
	for entry := range segmentEntries {
		if entry.IsDeleted {
			store.Delete(entry.Key)
		} else {
			store.Set(entry.Key, entry.Value)
		}
	}
}

func (db *DB) LoadStoreFromWAL() {
	segmentNames := db.wal.GetSegmentNames()
	for _, segmentName := range segmentNames {
		segment := db.wal.GetSegment(segmentName)
		db.updateStoreWithSegment(segment, db.kvStore)
	}
}

func NewDB() *DB {
	wal := NewWAL()
	db := &DB{kvStore: NewKeyValue(), wal: wal}
	db.LoadStoreFromWAL()
	return db
}
