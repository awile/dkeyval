package main

import (
	"bufio"
	"log"
	"os"
	"strings"
)

type WAL struct {
	appendFile *os.File
	readFile   *os.File
}

func (w *WAL) LoadStoreFromWAL() map[string]interface{} {
	store := make(map[string]interface{})
	scanner := bufio.NewScanner(w.readFile)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		parts := strings.Split(line, ",")
		key := parts[0]
		value := parts[1]
		is_deleted := parts[2]
		if is_deleted == "true" {
			delete(store, key)
		} else {
			store[key] = value
		}
	}

	return store
}

func NewWAL() *WAL {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	appendFile, err := os.OpenFile(pwd+"/data/wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	readFile, err := os.Open(pwd + "/data/wal.log")
	if err != nil {
		log.Fatal(err)
	}
	return &WAL{appendFile: appendFile, readFile: readFile}
}
