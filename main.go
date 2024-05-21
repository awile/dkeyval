package main

import "fmt"

func main() {
	wal := NewWAL()
	store := wal.LoadStoreFromWAL()
	kv := NewKeyValue(store)
	fmt.Println(kv.Get("name"))
	fmt.Println(kv.Get("other"))
	fmt.Println(kv.Get("key"))
}
