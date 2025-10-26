package main

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type DiskManager struct {
	file *os.File
}

func NewDiskManager(file *os.File) *DiskManager {
	return &DiskManager{
		file: file,
	}
}

func (dm *DiskManager) ReadFile(buf []byte, offset int64) error {
	_, err := dm.file.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("failed to read file: %w", err)
	}

	return nil
}

func (dm *DiskManager) WriteFile(buf []byte, offset int64) error {
	_, err := dm.file.WriteAt(buf, offset)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}
