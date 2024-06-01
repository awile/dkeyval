package main

import (
	"bufio"
	"fmt"
	"io"
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

func ParseToSegmentEntry(line string) SegmentEntry {
	parts := strings.Split(line, ",")
	key := parts[0]
	value := parts[1]
	is_deleted := parts[2] == "true"
	return SegmentEntry{Key: key, Value: value, IsDeleted: is_deleted}
}

// Segment
//////////

type SegmentData interface {
	io.Reader
	io.Writer
	Stat() (os.FileInfo, error)
	Close() error
}

type Segment struct {
	Name       string
	Data       SegmentData
	IsReadOnly bool
}

func (s *Segment) GetData() (c chan SegmentEntry) {
	c = make(chan SegmentEntry)
	go func() {
		scanner := bufio.NewScanner(s.Data)
		for scanner.Scan() {
			line := scanner.Text()
			if len(line) == 0 {
				continue
			}
			c <- ParseToSegmentEntry(line)
		}
		close(c)
	}()
	return c
}

func (s *Segment) WriteEntry(entry SegmentEntry) (err error) {
	writer := bufio.NewWriter(s.Data)
	defer writer.Flush()
	row := fmt.Sprintf(
		"%s,%s,%t\n",
		entry.Key, entry.Value, entry.IsDeleted,
	)
	_, err = writer.WriteString(row)
	if err != nil {
		return err
	}
	return nil
}

func (s *Segment) Size() (int64, error) {
	fileInfo, err := s.Data.Stat()
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func (s *Segment) Close() {
	s.Data.Close()
}

func NewSegment(name string, path string, isReadOnly bool) Segment {
	permissions := os.O_RDONLY
	if !isReadOnly {
		permissions = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	file, err := os.OpenFile(path+name, permissions, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return Segment{Name: name, Data: file, IsReadOnly: isReadOnly}
}

// SegmentManager
type SegmentManager struct {
	DataDir        string
	fileNameFormat string
}

func (sm *SegmentManager) ListSegmentNames() []string {
	// Return sorted list of segment names
	// Last item is the latest segment
	files, err := os.ReadDir(sm.DataDir)
	if err != nil {
		return []string{}
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

func (sm *SegmentManager) GetLatestSegment() *Segment {
	segments := sm.ListSegmentNames()
	if len(segments) == 0 {
		return nil
	}
	latestSegment := NewSegment(segments[len(segments)-1], sm.DataDir, false)
	return &latestSegment
}

func (sm *SegmentManager) GetSegment(segment string) Segment {
	return NewSegment(segment, sm.DataDir, true)
}

func (sm *SegmentManager) getNextSegmentName() (string, error) {
	latestSegment := sm.GetLatestSegment()
	if latestSegment == nil {
		return fmt.Sprintf(sm.fileNameFormat, 1), nil
	}
	segmentNumber, err := strconv.Atoi(strings.TrimSuffix(latestSegment.Name, ".log"))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(sm.fileNameFormat, segmentNumber+1), nil
}

func (sm *SegmentManager) CreateSegment() Segment {
	nextSegmentName, err := sm.getNextSegmentName()
	if err != nil {
		log.Fatal(err)
	}
	return NewSegment(nextSegmentName, sm.DataDir, false)
}

func NewSegmentManager() *SegmentManager {
	fileFormat := "%06d.log"
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	return &SegmentManager{DataDir: pwd + "/data/wal/", fileNameFormat: fileFormat}
}

// WAL
//////

type ISegmentManager interface {
	ListSegmentNames() []string
	GetSegment(segment string) Segment
	CreateSegment() Segment
	GetLatestSegment() *Segment
}

type WAL struct {
	SegmentManager ISegmentManager
	CurrentSegment *Segment
}

func (w *WAL) SetActiveSegment() {
	latestSegment := w.SegmentManager.GetLatestSegment()
	w.CurrentSegment = latestSegment
}

func (w *WAL) ListSegmentNames() []string {
	return w.SegmentManager.ListSegmentNames()
}

func (w *WAL) GetSegment(segment string) Segment {
	return w.SegmentManager.GetSegment(segment)
}

func (w *WAL) ShouldRotateActiveSegment() bool {
	size, err := w.CurrentSegment.Size()
	if err != nil {
		return false
	}
	return size > 1024
}

func (w *WAL) RotateSegment() {
	newSegment := w.SegmentManager.CreateSegment()
	w.CurrentSegment.Close()
	w.CurrentSegment = &newSegment
}

func (w *WAL) AppendToWAL(entry SegmentEntry) {
	w.CurrentSegment.WriteEntry(entry)
	if w.ShouldRotateActiveSegment() {
		w.RotateSegment()
	}
}

func NewWAL() *WAL {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	segementManager := SegmentManager{DataDir: pwd + "/data/wal/"}
	wal := WAL{SegmentManager: &segementManager, CurrentSegment: nil}
	wal.SetActiveSegment()
	return &wal
}
