package main

import (
	"fmt"
	"time"
)

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

func (db *DB) updateStoreWithSegment(segment Segment, store *KeyValue) {
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
	segmentNames := db.wal.ListSegmentNames()
	for _, segmentName := range segmentNames {
		segment := db.wal.GetSegment(segmentName)
		db.updateStoreWithSegment(segment, db.kvStore)
	}
}

func (db *DB) startCompaction() {
	for {
		totalSize, err := db.wal.GetTotalSize()
		fmt.Println("Total size: ", totalSize)
		if err == nil && totalSize > 3000 {
			// Only start compacting if total space > 3000
			// This is compiling into a single file, so is pretty inefficient now
			db.wal.CompactSegments()
		}
		time.Sleep(10 * time.Second)
	}
}

func (db *DB) startLogRotation() *KeyValue {
	for {
		if db.wal.ShouldRotateActiveSegment() {
			db.wal.RotateSegment()
		}
		time.Sleep(5 * time.Second)
	}
}

func (db *DB) Close() {
	db.wal.CurrentSegment.Close()
}

func NewDB() *DB {
	wal := NewWAL()
	db := &DB{kvStore: NewKeyValue(), wal: wal}
	db.LoadStoreFromWAL()
	go func(db *DB) {
		db.startCompaction()
	}(db)
	go func(db *DB) {
		db.startLogRotation()
	}(db)
	return db
}
