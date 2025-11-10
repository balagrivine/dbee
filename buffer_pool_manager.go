package main

import (
	"sync"
	"sync/atomic"
)

// BufferPoolManager is responsible for moving physical pages of data back
// and forth from buffers in main memory to persistent storage.
// It also behaves as a cache, keeping frequently used pages in memory for
// faster access, and evicting unused or cold pages back out to storage.
type BufferPoolManager struct {
	size           int
	storageManager storageManager
	bufferPool     []*Frame

	// pageTable keeps track of pages currently in memory.
	// It also maintains additional meta-data per page.
	pageTable map[int64]int

	// mu is a mutex used to latch pages in memory so that
	// the buffer pool manager does not evict a page from memory
	// when a query is using the page.
	mu *sync.Mutex
}

// Frame is a fixed-size slot in the buffer pool, a dedicated area
// of main memory, that holds a copy of a disk page.
type Frame struct {
	ID   int
	Data []byte

	// The ID of the disk page currently stored in the frame.
	pageID int64

	// pinCount is a counter to track how many processes are currently
	// using the page. The frame cannot be replaced if the pin count is greater than zero.
	pinCount atomic.Int64

	// isDirty is a flag bit used to tell if the page in the
	// frame has been modified since it was first loaded from disk.
	isDirty bool
}

type storageManager interface {
	ReadPage(pageID int64) ([]byte, error)
	WritePage(pageID int64, tuple []byte) error
}

func NewBufferPoolManager(size int, sm storageManager) *BufferPoolManager {
	return &BufferPoolManager{
		size:           size,
		mu:             &sync.Mutex{},
		storageManager: sm,
		pageTable:      make(map[int64]int),
		bufferPool:     make([]*Frame, 0),
	}
}

// FetchPage is invoked by the executor to fetch a page corresponding to pageID.
// If the page is found in memory, it will be immediately returned, else, the page
// is read from disk into memory and then returned.
func (bpm *BufferPoolManager) FetchPage(pageID int64) (*Frame, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	frameID, ok := bpm.pageTable[pageID]
	if ok {
		frame := bpm.bufferPool[frameID]
		frame.pinCount.Add(1)

		return frame, nil
	}

	frame := bpm.allocateFrame(1, pageID)

	data, err := bpm.readPageFromDisk(pageID)
	if err != nil {
		return nil, err
	}

	frame.Data = data

	return frame, nil
}

// readPageFromDisk fetches a page from disk if a frame holding a page
// corresponding to pageID is not found in memory.
func (bpm *BufferPoolManager) readPageFromDisk(pageID int64) ([]byte, error) {
	data, err := bpm.storageManager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (bpm *BufferPoolManager) allocateFrame(frameID int, pageID int64) *Frame {
	return &Frame{
		ID:       frameID,
		pageID:   pageID,
		isDirty:  false,
		Data:     nil,
		pinCount: atomic.Int64{},
	}
}
