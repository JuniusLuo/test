package test

import (
	"os"
	"strings"

	"github.com/golang/glog"
)

// FileIO is the test io engine for CloudIO, operates on the local file system
type FileIO struct {
	rootDir       string
	rootBucketDir string
	rootDataDir   string
}

// Misc const definition for FileIO
const (
	DefaultDirMode = 0700
)

// NewFileIO creates the FileIO instance
func NewFileIO() *FileIO {
	f := new(FileIO)
	f.rootDir = "/tmp/clouddd/"
	f.rootBucketDir = f.rootDir + "bucket/"
	f.rootDataDir = f.rootDir + "data"

	err := os.MkdirAll(f.rootBucketDir, DefaultDirMode)
	if err != nil && !os.IsExist(err) {
		glog.Errorln("failed to create", f.rootBucketDir, err)
		return nil
	}

	err = os.MkdirAll(f.rootDataDir, DefaultDirMode)
	if err != nil && !os.IsExist(err) {
		glog.Errorln("failed to create", f.rootDataDir, err)
		return nil
	}

	return f
}

// PutBucket creates the target bucket
func (f *FileIO) PutBucket(bkname string) (status int, errmsg string) {
	path := f.rootBucketDir + bkname
	err := os.Mkdir(path, DefaultDirMode)
	if err != nil {
		glog.Errorln("failed to create bucket dir", path, err)
		if os.IsExist(err) {
			return BucketAlreadyExists, "BucketAlreadyExists"
		}
		return InternalError, "failed to create bucket dir"
	}

	return StatusOK, StatusOKStr
}

// DeleteBucket deletes the target bucket
func (f *FileIO) DeleteBucket(bkname string) (status int, errmsg string) {
	path := f.rootBucketDir + bkname
	err := os.Remove(path)
	if err != nil {
		glog.Errorln("failed to delete bucket dir", path, err)
		if os.IsNotExist(err) {
			return NoSuchBucket, "NoSuchBucket"
		}
		if strings.Contains(err.Error(), "not empty") {
			return BucketNotEmpty, "BucketNotEmpty"
		}
		return InternalError, "failed to delete bucket"
	}
	return StatusOK, StatusOKStr
}
