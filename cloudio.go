package test

// CloudIO defines the ioengine interfaces
type CloudIO interface {
	PutBucket(bkname string) (status int, errmsg string)
	DeleteBucket(bkname string) (status int, errmsg string)
}
