package main

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testFile(t *testing.T) *os.File {
	t.Helper()

	file, err := os.CreateTemp("", "test.db")
	assert.NoError(t, err)

	return file
}

type mockDiskManager struct {
	wantErr bool
	file    *os.File
	result  []byte
}

func (md *mockDiskManager) Read(file *os.File, buf []byte, offset int64) error {
	if md.wantErr {
		return errors.New("failed to read from file")
	}

	buf = md.result

	return nil
}

func (md *mockDiskManager) Write(file *os.File, buf []byte, offset int64) error {
	if md.wantErr {
		return errors.New("failed to write to file")
	}

	return nil
}

func TestStorageManager_InsertTuple(t *testing.T) {
	type fields struct {
		diskManager diskManager
	}
	type args struct {
		file   *os.File
		pageID int
		tuple  []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Happy case: successfully inserted a tuple into a page",
			fields: fields{
				diskManager: &mockDiskManager{
					file:    testFile(t),
					wantErr: false,
				},
			},
			args: args{
				pageID: 1,
				tuple:  make([]byte, PageSize),
			},
			wantErr: false,
		},
		{
			name: "Sad case: failed to insert tuple into a page",
			fields: fields{
				diskManager: &mockDiskManager{
					file:    testFile(t),
					wantErr: true,
				},
			},
			args: args{
				file:   testFile(t),
				pageID: 1,
				tuple:  make([]byte, PageSize),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &StorageManager{
				diskManager: tt.fields.diskManager,
			}
			if err := sm.InsertTuple(tt.args.file, tt.args.pageID, tt.args.tuple); (err != nil) != tt.wantErr {
				t.Errorf("StorageManager.InsertTuple() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStorageManager_ReadTuple(t *testing.T) {
	type fields struct {
		diskManager diskManager
	}
	type args struct {
		file   *os.File
		pageID int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "Happy case: successfully read a tuple from a page",
			fields: fields{
				diskManager: &mockDiskManager{
					file:    testFile(t),
					wantErr: false,
					result:  []byte("Tuple read successfully"),
				},
			},
			args: args{
				file:   testFile(t),
				pageID: 1,
			},
			wantErr: false,
			want:    []byte("Tuple read successfully"),
		},
		{
			name: "Sad case: failed to read tuple from a page",
			fields: fields{
				diskManager: &mockDiskManager{
					file:    testFile(t),
					wantErr: true,
				},
			},
			args: args{
				file:   testFile(t),
				pageID: 1,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &StorageManager{
				diskManager: tt.fields.diskManager,
			}
			_, err := sm.ReadTuple(tt.args.file, tt.args.pageID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("StorageManager.ReadTuple() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
		})
	}
}
