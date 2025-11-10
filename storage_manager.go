package main

import (
	"fmt"
)

const (
	PageSize       int64 = 4 * 1024 // 4KB
	PageHeaderSize int   = 48       // 48 bytes
)

// diskManager provides an abstraction layer, shielding the rest of the storage engine
// from details of the underlying disk.
type diskManager interface {
	Read(buf []byte, offset int64) error
	Write(buf []byte, offset int64) error
}

// StorageManager deals with how data is structured logically on disk.
type StorageManager struct {
	diskManager diskManager
}

func NewStorageManager(diskManager diskManager) *StorageManager {
	return &StorageManager{
		diskManager: diskManager,
	}
}

// ReadPage reads a page with id pageID from the database file.
func (sm *StorageManager) ReadPage(pageID int64) ([]byte, error) {
	pageOffset := PageSize * pageID
	pageBuff := make([]byte, PageSize)

	err := sm.diskManager.Read(pageBuff, pageOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page contents: %w", err)
	}

	return pageBuff, nil
}

// WritePage writes data to a page with id pageID
// Given the constant page size of 4KB, we are guaranteed that the write operation will
// happen atomically.
func (sm *StorageManager) WritePage(pageID int64, tuple []byte) error {
	pageOffset := PageSize * pageID

	err := sm.diskManager.Write(tuple, pageOffset)
	if err != nil {
		return fmt.Errorf("error writing to page %d: %w", pageID, err)
	}

	return nil
}
