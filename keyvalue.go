package main

type KeyValue struct {
	store map[string]interface{}
}

func (kv *KeyValue) Set(key string, value interface{}) {
	kv.store[key] = value
}

func (kv *KeyValue) Get(key string) interface{} {
	return kv.store[key]
}

func (kv *KeyValue) Delete(key string) {
	delete(kv.store, key)
}

func (kv *KeyValue) Merge(otherStore *KeyValue) {
	for key, value := range otherStore.store {
		kv.store[key] = value
	}
}

func NewKeyValue() *KeyValue {
	store := make(map[string]interface{})
	return &KeyValue{store: store}
}
