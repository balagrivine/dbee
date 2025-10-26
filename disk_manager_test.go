package main

import (
	"os"
	"testing"
)

func TestDiskManager_ReadFile(t *testing.T) {
	type fields struct {
		file *os.File
	}
	type args struct {
		buf    []byte
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Successfully read file contents from a file",
			fields: fields{
				file: testFile(t),
			},
			args: args{
				buf:    make([]byte, PageSize),
				offset: 1,
			},
			wantErr: false,
		},
		{
			name: "Sad case: failed to read file contents - read from a non-existent file",
			fields: fields{
				file: nil,
			},
			args: args{
				buf:    make([]byte, PageSize),
				offset: 1,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dm := &DiskManager{
				file: tt.fields.file,
			}
			if err := dm.ReadFile(tt.args.buf, tt.args.offset); (err != nil) != tt.wantErr {
				t.Errorf("DiskManager.ReadFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDiskManager_WriteFile(t *testing.T) {
	type fields struct {
		file *os.File
	}
	type args struct {
		buf    []byte
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Successfully written file contents from a file",
			fields: fields{
				file: testFile(t),
			},
			args: args{
				buf:    make([]byte, PageSize),
				offset: 1,
			},
			wantErr: false,
		},
		{
			name: "Sad case: failed to write file contents - read from a non-existent file",
			fields: fields{
				file: nil,
			},
			args: args{
				buf:    make([]byte, PageSize),
				offset: 1,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dm := &DiskManager{
				file: tt.fields.file,
			}
			if err := dm.WriteFile(tt.args.buf, tt.args.offset); (err != nil) != tt.wantErr {
				t.Errorf("DiskManager.WriteFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
