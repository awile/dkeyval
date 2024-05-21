package main

import "fmt"

func main() {
	db := NewDB()

	fmt.Println(db.Get("name"))
	fmt.Println(db.Get("age"))
}
