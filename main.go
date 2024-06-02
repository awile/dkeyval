package main

import (
	"fmt"
	"time"
)

func main() {
	db := NewDB()

	fmt.Println(db.Get("something"))
	fmt.Println(db.Get("magic"))
	time.Sleep(4 * time.Second)
	fmt.Println(db.Get("something"))
	fmt.Println(db.Get("magic"))
}
