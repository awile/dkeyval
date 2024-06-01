package main_test

import (
	"io"
	"os"
	"testing"
	"time"

	. "github.com/awile/dkeyval"
)

// SegmentEntry
func TestParseToSegmentEntry(t *testing.T) {
	line := "key,value,true"
	entry := ParseToSegmentEntry(line)
	if entry.Key != "key" {
		t.Errorf("Expected key to be 'key', got %s", entry.Key)
	}
	if entry.Value != "value" {
		t.Errorf("Expected value to be 'value', got %s", entry.Value)
	}
	if !entry.IsDeleted {
		t.Errorf("Expected is_deleted to be true, got false")
	}
}

type MockFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	sys     interface{}
}

func (m MockFileInfo) Name() string {
	return m.name
}

func (m MockFileInfo) Size() int64 {
	return m.size
}

func (m MockFileInfo) Mode() os.FileMode {
	return m.mode
}

func (m MockFileInfo) ModTime() time.Time {
	return m.modTime
}

func (m MockFileInfo) IsDir() bool {
	return m.isDir
}

func (m MockFileInfo) Sys() interface{} {
	return m.sys
}

// Segment
type MockSegmentData struct {
	Data string
	done bool
}

func (m *MockSegmentData) Read(p []byte) (n int, err error) {
	copy(p, []byte(m.Data))
	if m.done {
		return 0, io.EOF
	}
	m.done = true
	return len([]byte(m.Data)), nil
}

func (m *MockSegmentData) Write(p []byte) (n int, err error) {
	m.Data = string(p)
	return len(p), nil
}

func (m *MockSegmentData) Stat() (os.FileInfo, error) {
	size := int64(len(m.Data))
	fileInfo := MockFileInfo{name: "mock", size: size, mode: 0, modTime: time.Time{}, isDir: false, sys: nil}
	return fileInfo, nil
}

func (m *MockSegmentData) Close() error {
	return nil
}

func NewMockSegment(data string) *Segment {
	return &Segment{Name: "mock", Data: &MockSegmentData{Data: data}, IsReadOnly: true}
}

func TestSegmentGetData(t *testing.T) {
	segment := NewMockSegment("key1,value1,false\nkey2,value2,true\n")
	c := segment.GetData()
	actual_entries := []SegmentEntry{}
	expected_entries := []SegmentEntry{
		{Key: "key1", Value: "value1", IsDeleted: false},
		{Key: "key2", Value: "value2", IsDeleted: true},
	}
	for entry := range c {
		actual_entries = append(actual_entries, entry)
	}

	for i, entry := range actual_entries {
		if entry.Key != expected_entries[i].Key {
			t.Errorf("Expected key %s, got %s", expected_entries[i].Key, entry.Key)
		}
		if entry.Value != expected_entries[i].Value {
			t.Errorf("Expected value %s, got %s", expected_entries[i].Value, entry.Value)
		}
		if entry.IsDeleted != expected_entries[i].IsDeleted {
			t.Errorf("Expected is_deleted %t, got %t", expected_entries[i].IsDeleted, entry.IsDeleted)
		}
	}
}

func TestWriteEntry(t *testing.T) {
	mockSegmentData := &MockSegmentData{Data: ""}
	segment := Segment{Name: "mock", Data: mockSegmentData, IsReadOnly: false}
	entry := SegmentEntry{Key: "key1", Value: "value1", IsDeleted: false}
	result := segment.WriteEntry(entry)
	if result != nil {
		t.Errorf("Expected no error, got %s", result)
	}
	expected := "key1,value1,false\n"
	if mockSegmentData.Data != expected {
		t.Errorf("Expected data to be %s, got %s", expected, mockSegmentData.Data)
	}
}

func TestSize(t *testing.T) {
	value := "key1,value1,false\nkey2,value2,true\n"
	segment := NewMockSegment(value)
	size, _ := segment.Size()
	if size != int64(len(value)) {
		t.Errorf("Expected size to be %d, got %d", len(value), size)
	}
}
