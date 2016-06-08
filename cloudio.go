package test

import "net/http"

// CloudIO defines the ioengine interfaces
type CloudIO interface {
	PutBucket(bkname string) (status int, errmsg string)
	DeleteBucket(bkname string) (status int, errmsg string)
	GetBucket(bkname string, resp *http.Response)
	HeadBucket(bkname string) (status int, errmsg string)

	IsDataBlockExist(md5str string) bool
	WriteDataBlock(buf []byte, md5str string) (status int, errmsg string)
	ReadDataBlockRange(md5str string, off int64, b []byte) (n int, status int, errmsg string)

	// to reduce the bucket list latency, WriteObjectMD should store the default
	// list metadata as the usermd of S3 object. So bucket list doesn't need to
	// fetch the content of every object.
	WriteObjectMD(bkname string, objname string, mdbuf []byte) (status int, errmsg string)
	ReadObjectMD(bkname string, objname string) (b []byte, status int, errmsg string)
}
