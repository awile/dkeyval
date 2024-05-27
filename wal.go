package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

// SegmentEntry
///////////////

type SegmentEntry struct {
	Key       string
	Value     interface{}
	IsDeleted bool
}

func ParseSegmentEntry(line string) SegmentEntry {
	parts := strings.Split(line, ",")
	key := parts[0]
	value := parts[1]
	is_deleted := parts[2] == "true"
	return SegmentEntry{Key: key, Value: value, IsDeleted: is_deleted}
}

// Segment
//////////

type Segment struct {
	Name string
	file *os.File
}

func (s *Segment) GetData() (c chan SegmentEntry) {
	c = make(chan SegmentEntry)
	go func() {
		scanner := bufio.NewScanner(s.file)
		for scanner.Scan() {
			line := scanner.Text()
			if len(line) == 0 {
				continue
			}
			c <- ParseSegmentEntry(line)
		}
		close(c)
	}()
	return c
}

func (s *Segment) writeEntry(entry SegmentEntry) {
	writer := bufio.NewWriter(s.file)
	defer writer.Flush()
	row := fmt.Sprintf(
		"%s,%s,%t\n",
		entry.Key, entry.Value, entry.IsDeleted,
	)
	_, err := writer.WriteString(row)
	if err != nil {
		log.Fatal(err)
	}
}

func (s *Segment) Size() int64 {
	fileInfo, err := s.file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	return fileInfo.Size()
}

func (s *Segment) Close() {
	s.file.Close()
}

func NewSegment(name string, isReadOnly bool, path string) *Segment {
	fileMode := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	if isReadOnly {
		fileMode = os.O_RDONLY
	}
	file, err := os.OpenFile(path+name, fileMode, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return &Segment{Name: name, file: file}
}


// WAL
//////

type WAL struct {
	dataDir        string
	currentSegment *Segment
}

func (w *WAL) getLatestSegmentName() (string, error) {
	segments := w.GetSegmentNames()
	if len(segments) == 0 {
		return "", fmt.Errorf("No segments found")
	}
	return segments[len(segments)-1], nil
}

func (w *WAL) getNextSegmentName() string {
	fileNameFormat := "%06d.log"
	latestSegment, err := w.getLatestSegmentName()
	if err != nil {
		return fmt.Sprintf(fileNameFormat, 1)
	}
	segmentNumber, err := strconv.Atoi(strings.TrimSuffix(latestSegment, ".log"))
	if err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf(fileNameFormat, segmentNumber+1)
}

func (w *WAL) createNewSegment() *Segment {
	newSegmentName := w.getNextSegmentName()
	return NewSegment(newSegmentName, false, w.dataDir)
}

func (w *WAL) shouldRotateSegment() bool {
	return w.currentSegment.Size() > 1024
}

func (w *WAL) rotateSegment() {
	w.currentSegment.Close()
	w.currentSegment = w.createNewSegment()
}

func (w *WAL) GetSegmentNames() []string {
	files, err := os.ReadDir(w.dataDir)
	if err != nil {
		log.Fatal(err)
	}
	segments := []string{}
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".log") {
			segments = append(segments, file.Name())
		}
	}
	sort.Strings(segments)
	return segments
}

func (w *WAL) GetLatestSegment() *Segment {
	latestSegment, err := w.getLatestSegmentName()
	if err != nil {
		return w.createNewSegment()
	}
	return NewSegment(latestSegment, false, w.dataDir)
}

func (w *WAL) GetSegment(segment string) *Segment {
	return NewSegment(segment, true, w.dataDir)
}

func (w *WAL) AppendToWAL(entry SegmentEntry) {
	w.currentSegment.writeEntry(entry)
	if w.shouldRotateSegment() {
		w.rotateSegment()
	}
}

func NewWAL() *WAL {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	wal := WAL{dataDir: pwd + "/data/wal/", currentSegment: nil}
	wal.currentSegment = wal.GetLatestSegment()
	return &wal
}
