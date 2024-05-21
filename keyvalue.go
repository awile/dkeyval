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


func NewKeyValue() *KeyValue {
  return &KeyValue{store: make(map[string]interface{})}
}
