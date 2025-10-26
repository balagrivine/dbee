package main

import (
	"fmt"
	"os"
)

const (
	PageSize       int = 4 * 1024 // 4KB
	PageHeaderSize int = 48       // 48 bytes
)

// diskManager provides an abstraction layer, shielding the rest of the storage engine
// from details of the underlying disk.
type diskManager interface {
	Read(file *os.File, buf []byte, offset int64) error
	Write(file *os.File, buf []byte, offset int64) error
}

// StorageManager deals with how data is structured logically on disk.
type StorageManager struct {
	diskManager diskManager
	PageSize    int
}

func NewStorageManager(diskManager diskManager) *StorageManager {
	return &StorageManager{
		diskManager: diskManager,
		PageSize:    PageSize,
	}
}

// ReadTuple reads a tuple from file, within the page specified by pageID.
func (sm *StorageManager) ReadTuple(file *os.File, pageID int) ([]byte, error) {
	tuple := make([]byte, sm.PageSize)
	pageOffset := int64(pageID * sm.PageSize)

	err := sm.diskManager.Read(file, tuple, pageOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page contents: %w", err)
	}

	return tuple, nil
}

// InsertTuple inserts a tuple inside file, within the page specified by pageID
// Given the constant page size of 4KB, we are guaranteed that the write operation will
// happen atomically.
func (sm *StorageManager) InsertTuple(file *os.File, pageID int, tuple []byte) error {
	pageOffset := int64(pageID * sm.PageSize)

	err := sm.diskManager.Write(file, tuple, pageOffset)
	if err != nil {
		return fmt.Errorf("error inserting tuple to page %d: %w", pageID, err)
	}

	return nil
}
