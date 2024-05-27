package main

import "fmt"

func main() {
	db := NewDB()

	fmt.Println(db.Get("test_huh"))
}
