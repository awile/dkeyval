package main

import (
	"fmt"
	"time"
)

func main() {
	db := NewDB()

	fmt.Println(db.Get("something"))
	fmt.Println(db.Get("magic"))
	for i := 0; i < 1000; i++ {
		db.Set(fmt.Sprintf("other%d", i), fmt.Sprintf("some%d", i))
		time.Sleep(250 * time.Millisecond)
	}
	time.Sleep(4 * time.Second)
	fmt.Println(db.Get("something"))
	fmt.Println(db.Get("magic"))
}
