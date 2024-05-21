package main


import "fmt"


func main() {
  kv := NewKeyValue()
  kv.Set("name", "John")
  kv.Set("other", "John")
  kv.Set("name", "Tom")
  fmt.Println(kv.Get("name"))
}
