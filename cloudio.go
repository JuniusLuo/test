package test

import "net/http"

// CloudIO defines the ioengine interfaces
type CloudIO interface {
	PutBucket(bkname string) (status int, errmsg string)
	DeleteBucket(bkname string) (status int, errmsg string)
	GetBucket(bkname string, resp *http.Response)

	IsDataBlockExist(md5str string) bool
	WriteDataBlock(buf []byte, md5str string) (status int, errmsg string)

	WriteObjectMD(bkname string, objname string, mdbuf []byte) (status int, errmsg string)
}
