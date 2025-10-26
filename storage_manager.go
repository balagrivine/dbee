package main

import "fmt"

const (
	PageSize = 4 * 1024
)

// diskManager provides an abstraction layer, shielding the rest of the storage engine
// from details of the underlying disk
type diskManager interface {
	ReadFile(buf []byte, offset int64) error
	WriteFile(buf []byte, offset int64) error
}

// StorageManager deals with how data is structured logically on disk
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

// ReadTuple reads a tuple from the page specified by pageID
func (sm *StorageManager) ReadTuple(pageID int) ([]byte, error) {
	tuple := make([]byte, sm.PageSize)
	pageOffset := int64(pageID * sm.PageSize)

	err := sm.diskManager.ReadFile(tuple, pageOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page contents: %w", err)
	}

	return tuple, nil
}

// InsertTuple inserts a tuple inside the page specified by pageID
// Given the constant page size of 4KB, we are guaranteed that the write operation will
// happen atomically.
func (sm *StorageManager) InsertTuple(pageID int, tuple []byte) error {
	pageOffset := int64(pageID * sm.PageSize)

	err := sm.diskManager.WriteFile(tuple, pageOffset)
	if err != nil {
		return fmt.Errorf("error inserting tuple to page %d: %w", pageID, err)
	}

	return nil
}
