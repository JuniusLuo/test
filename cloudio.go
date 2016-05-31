package test

// CloudIO defines the ioengine interfaces
type CloudIO interface {
	PutBucket(bkname string) (status int, errmsg string)
	DeleteBucket(bkname string) (status int, errmsg string)

	IsDataBlockExist(md5str string) bool
	WriteDataBlock(buf []byte, md5str string) (status int, errmsg string)

	WriteObjectMD(bkname string, objname string, mdbuf []byte) (status int, errmsg string)
}
