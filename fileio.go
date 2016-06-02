package test

import (
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

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
	DefaultDirMode  = 0700
	DefaultFileMode = 0600
)

// NewFileIO creates the FileIO instance
func NewFileIO() *FileIO {
	f := new(FileIO)
	f.rootDir = "/tmp/clouddd/"
	f.rootBucketDir = f.rootDir + "bucket/"
	f.rootDataDir = f.rootDir + "data/"

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

// The IOReader for object list
type listObjectIOReader struct {
	xmlbyte []byte
	// the last read position
	r int
}

func (l *listObjectIOReader) Close() error {
	return nil
}

func (l *listObjectIOReader) Read(p []byte) (n int, err error) {
	glog.V(4).Infoln("listObjectIOReader p len", len(p), "xmlbyte len", len(l.xmlbyte), l.r)

	if l.r >= len(l.xmlbyte) {
		// all readed, return 0 + EOF
		return 0, io.EOF
	}

	t := l.r + len(p)
	if t >= len(l.xmlbyte) {
		copy(p, l.xmlbyte[l.r:])
		t = len(l.xmlbyte) - l.r
		l.r = len(l.xmlbyte)
		return t, io.EOF
	}

	copy(p, l.xmlbyte[l.r:t])
	l.r = t
	return len(p), nil
}

// GetBucket lists objects in the bucket.
// Only support the simple list
func (f *FileIO) GetBucket(bkname string, resp *http.Response) {
	type Content struct {
		XMLName      xml.Name `xml:"Contents"`
		Key          string   `xml:"Key"`
		LastModified string   `xml:"LastModified"`
		ETag         string   `xml:"ETag"`
		Size         int64    `xml:"Size"`
		StorageClass string   `xml:"StorageClass"`
	}

	type ListBucketResult struct {
		XMLName     xml.Name  `xml:"ListBucketResult"`
		Xmlns       string    `xml:"xmlns,attr"`
		Name        string    `xml:"Name"`
		Prefix      string    `xml:"Prefix"`
		KeyCount    int       `xml:"KeyCount"`
		MaxKeys     int       `xml:"MaxKeys"`
		IsTruncated bool      `xml:"IsTruncated"`
		Contents    []Content `xml:"Contents"`
	}

	dirpath := f.rootBucketDir + bkname
	fd, err := os.Open(dirpath)
	if err != nil {
		glog.Errorln("failed to open bucket dir", dirpath, err)
		resp.StatusCode = InternalError
		resp.Status = InternalErrorStr
		return
	}

	files, err := fd.Readdir(BucketListMaxKeys)
	fd.Close()
	if err != nil {
		glog.Errorln("failed to read bucket dir", dirpath, err)
		resp.StatusCode = InternalError
		resp.Status = InternalErrorStr
		return
	}

	glog.V(2).Infoln("list bucket dir, files", len(files), dirpath)

	res := &ListBucketResult{Xmlns: XMLNS, Name: bkname, KeyCount: len(files),
		MaxKeys: BucketListMaxKeys, IsTruncated: false}

	for _, fi := range files {
		c := Content{Key: fi.Name(), LastModified: fi.ModTime().Format(time.RFC3339),
			ETag: "ETag", Size: fi.Size(), StorageClass: "STANDARD"}
		res.Contents = append(res.Contents, c)
	}

	b, err := xml.Marshal(res)
	if err != nil {
		glog.Errorln("failed to marshal ListBucketResult", res, err)
		resp.StatusCode = InternalError
		resp.Status = InternalErrorStr
		return
	}

	glog.V(4).Infoln("ListBucketResult", res)

	rd := new(listObjectIOReader)
	rd.xmlbyte = b

	resp.Body = rd
	resp.StatusCode = StatusOK
	resp.Status = StatusOKStr
}

// IsDataBlockExist checks if the data block exists
func (f *FileIO) IsDataBlockExist(md5str string) bool {
	fname := f.rootDataDir + md5str
	_, err := os.Stat(fname)
	if err == nil {
		return true
	}
	glog.V(4).Infoln("data block not exist", md5str, err)
	return false
}

// WriteDataBlock creates the data block under the data bucket
func (f *FileIO) WriteDataBlock(buf []byte, md5str string) (status int, errmsg string) {
	fname := f.rootDataDir + md5str
	err := ioutil.WriteFile(fname, buf, DefaultFileMode)
	if err != nil {
		glog.Errorln("failed to write file", fname, err)
		return InternalError, "failed to create data block file"
	}
	return StatusOK, StatusOKStr
}

// WriteObjectMD creates the metadata object
func (f *FileIO) WriteObjectMD(bkname string, objname string, mdbuf []byte) (status int, errmsg string) {
	fname := f.rootBucketDir + bkname + objname
	err := ioutil.WriteFile(fname, mdbuf, DefaultFileMode)
	if err != nil {
		glog.Errorln("failed to create metadata object file", fname, err)
		return InternalError, "failed to create metadata file"
	}
	return StatusOK, StatusOKStr
}
