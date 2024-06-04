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

func getSegmentNumber(segment string) int {
	value := -1
	segmentToCheck := strings.TrimSuffix(strings.TrimPrefix(segment, "compacted_"), ".log")
	if strings.Contains(segment, "compacted_") {
		items := strings.Split(segmentToCheck, "_")
		segmentToCheck = items[len(items)-1]
	}
	value, err := strconv.Atoi(segmentToCheck)
	if err != nil {
		return -1
	}
	return value
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
	sort.Slice(segments, func(i, j int) bool {
		strings.Contains("GeeksforGeeks", "for")
		return getSegmentNumber(segments[i]) < getSegmentNumber(segments[j])
	})
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

func (sm *SegmentManager) DeleteSegment(segmentName string) {
	segment := sm.GetSegment(segmentName)
	segment.Close()
	err := os.Remove(sm.DataDir + segment.Name)
	if err != nil {
		log.Fatal(err)
	}
}

func (sm *SegmentManager) getMergedEntries(segment1 Segment, segment2 Segment) []SegmentEntry {
	mergedKeys := make(map[string]SegmentEntry)
	for entry := range segment1.GetData() {
		if entry.IsDeleted {
			delete(mergedKeys, entry.Key)
		} else {
			mergedKeys[entry.Key] = entry
		}
	}
	for entry := range segment2.GetData() {
		if entry.IsDeleted {
			delete(mergedKeys, entry.Key)
		} else {
			mergedKeys[entry.Key] = entry
		}
	}
	mergedEntries := []SegmentEntry{}
	for _, entry := range mergedKeys {
		mergedEntries = append(mergedEntries, entry)
	}
	return mergedEntries
}

func (sm *SegmentManager) createMergedSegment(segment1 string, segment2 string) *Segment {
	segment1Number := getSegmentNumber(segment1)
	segment2Number := getSegmentNumber(segment2)
	mergedSegmentName := fmt.Sprintf("compacted_%d_%d.log", segment1Number, segment2Number)
	mergedSegment := NewSegment(mergedSegmentName, sm.DataDir, false)
	return &mergedSegment
}

func (sm *SegmentManager) MergeSegments(segment1 string, segment2 string) {
	segmentData1 := sm.GetSegment(segment1)
	segmentData2 := sm.GetSegment(segment2)
	mergedEntries := sm.getMergedEntries(segmentData1, segmentData2)
	mergedSegment := sm.createMergedSegment(segment1, segment2)
	if mergedSegment == nil {
		return
	}
	for _, entry := range mergedEntries {
		mergedSegment.WriteEntry(entry)
	}
	sm.DeleteSegment(segment1)
	sm.DeleteSegment(segment2)
	mergedSegment.Close()
}

func NewSegmentManager() SegmentManager {
	fileFormat := "%06d.log"
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	return SegmentManager{DataDir: pwd + "/data/wal/", fileNameFormat: fileFormat}
}

// WAL
//////

type ISegmentManager interface {
	ListSegmentNames() []string
	DeleteSegment(segment string)
	GetSegment(segment string) Segment
	CreateSegment() Segment
	GetLatestSegment() *Segment
	MergeSegments(segment1 string, segment2 string)
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
}

func (w *WAL) GetTotalSize() (int64, error) {
	segments := w.ListSegmentNames()
	totalSize := int64(0)
	for _, segment := range segments {
		fullSegment := w.SegmentManager.GetSegment(segment)
		segmentSize, err := fullSegment.Size()
		if err != nil {
			return 0, err
		}
		totalSize += segmentSize
	}
	return totalSize, nil
}

func (w *WAL) CompactSegments() {
	currentSegmentName := w.CurrentSegment.Name
	segments := w.SegmentManager.ListSegmentNames()
	nonActiveSegments := []string{}
	for _, segment := range segments {
		if segment != currentSegmentName {
			nonActiveSegments = append(nonActiveSegments, segment)
		}
	}
	if len(nonActiveSegments) >= 2 {
		w.SegmentManager.MergeSegments(nonActiveSegments[0], nonActiveSegments[1])
	}
}

func NewWAL() *WAL {
	segementManager := NewSegmentManager()
	wal := WAL{SegmentManager: &segementManager, CurrentSegment: nil}
	wal.SetActiveSegment()
	return &wal
}
