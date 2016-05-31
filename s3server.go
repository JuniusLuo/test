package test

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
)

var ioengine = flag.String("io", "fileio", "the cloud ioengine: fileio or cloudio")

// S3Server handles the coming S3 requests
type S3Server struct {
	s3io CloudIO
}

// NewS3Server allocates a new S3Server instance
func NewS3Server() *S3Server {
	s := new(S3Server)
	if *ioengine == "fileio" {
		fio := NewFileIO()
		if fio == nil {
			glog.Errorln("failed to create CloudIO instance, type", *ioengine)
			return nil
		}
		s.s3io = fio
	}

	glog.Infoln("created S3Server, type", *ioengine)
	return s
}

func (s *S3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp := s.newResponse()
	bkname, objname := s.getBucketAndObjectName(r)

	glog.V(2).Infoln(r.Method, r.URL, r.Host, bkname, objname)

	if bkname == "" {
		glog.Errorln("InvalidRequest, no bucketname", r.Method, r.URL, r.Host)
		resp.StatusCode = InvalidRequest
		resp.Status = "InvalidRequest, no bucketname"
		resp.Write(w)
		return
	}

	switch r.Method {
	case "POST":
	case "PUT":
		s.putOp(r, resp, bkname, objname)
	case "GET":
		s.getOp(r, resp, bkname, objname)
	case "HEAD":
	case "DELETE":
		s.delOp(r, resp, bkname, objname)
	case "OPTIONS":
	default:
		glog.Errorln("unsupported request", r.Method, r.URL)
		resp.StatusCode = InvalidRequest
	}

	glog.Flush()

	resp.Write(w)
}

func (s *S3Server) newResponse() *http.Response {
	resp := new(http.Response)
	resp.ProtoMajor = 1
	resp.ProtoMinor = 1
	resp.StatusCode = StatusOK
	return resp
}

// S3 supports 2 types url.
// virtual-hosted–style: http://bucket.s3-aws-region.amazonaws.com
// path-style: http://s3-aws-region.amazonaws.com/bucket
func (s *S3Server) getBucketFromHost(host string) (bkname string) {
	urls := strings.Split(host, ".")

	if len(urls) == 3 {
		// path-style url
		return ""
	}

	if len(urls) == 4 {
		// check ip address or virtual-hosted-style url
		if strings.HasPrefix(urls[1], "s3") {
			// ip address
			return urls[0]
		}

		return ""
	}

	// TODO invalid URL?
	return ""
}

// get bucket and object name from request
func (s *S3Server) getBucketAndObjectName(r *http.Request) (bkname string, objname string) {
	bkname = s.getBucketFromHost(r.Host)

	if bkname == "" {
		// path-style url, get bucket name and object name from URL
		// url like /b1/k1 will be split to 3 elements, [ b1 k1]
		strs := strings.SplitN(r.URL.String(), "/", 3)
		l := len(strs)
		if l == 3 {
			return strs[1], "/" + strs[2]
		} else if l == 2 {
			return strs[1], ""
		} else {
			return "", ""
		}
	} else {
		// bucket is in r.Host, the whole URL is object name
		return bkname, r.URL.String()
	}
}

func (s *S3Server) isBucketOp(objname string) bool {
	if objname == "" || objname == "/" || objname == BucketListOp {
		return true
	}
	return false
}

func (s *S3Server) putOp(r *http.Request, resp *http.Response, bkname string, objname string) {
	if s.isBucketOp(objname) {
		resp.StatusCode, resp.Status = s.s3io.PutBucket(bkname)
		glog.Infoln("put bucket", r.URL, r.Host, bkname, resp.StatusCode, resp.Status)
	} else {
		s.putObject(r, resp, bkname, objname)
	}
}

func (s *S3Server) readFullBuf(r *http.Request, readBuf []byte) (n int, err error) {
	readZero := false
	var rlen int
	for rlen < len(readBuf) {
		n, err = r.Body.Read(readBuf[rlen:])
		rlen += n

		if err != nil {
			return rlen, err
		}

		if n == 0 && err == nil {
			if readZero {
				glog.Errorln("read 0 bytes from http with nil error twice", r.URL, r.Host)
				return rlen, err
			}
			// allow read 0 byte with nil error once
			readZero = true
		}
	}
	return rlen, err
}

// read object data and create data blocks.
// this func will update data blocks and etag in ObjectMD
func (s *S3Server) putObjectData(r *http.Request, md *ObjectMD) (status int, errmsg string) {
	var readBuf []byte
	if r.ContentLength > ReadBufferSize {
		readBuf = make([]byte, ReadBufferSize)
	} else {
		readBuf = make([]byte, r.ContentLength)
	}

	md5ck := md5.New()
	etag := md5.New()

	var totalBlocks int
	var ddBlocks int

	var rlen int64
	for rlen < r.ContentLength {
		// read one block
		n, err := s.readFullBuf(r, readBuf)
		rlen += int64(n)
		glog.V(4).Infoln("read", n, err, "total readed len", rlen,
			"specified read len", r.ContentLength, md.Smd.Bucket, md.Smd.Name)

		if err != nil {
			if err != io.EOF {
				glog.Errorln("failed to read data from http", err, "readed len", rlen,
					"ContentLength", r.ContentLength, md.Smd.Bucket, md.Smd.Name)
				return InternalError, "failed to read data from http"
			}

			// EOF, check if all contents are readed
			if rlen != r.ContentLength {
				glog.Errorln("read", rlen, "less than ContentLength",
					r.ContentLength, md.Smd.Bucket, md.Smd.Name)
				return InvalidRequest, "data less than ContentLength"
			}

			// EOF, check if the last data block is 0
			if n == 0 {
				break // break the for loop
			}

			// write out the last data block
		}

		// compute checksum
		md5ck.Write(readBuf[:n])
		md5byte := md5ck.Sum(nil)
		md5str := hex.EncodeToString(md5byte)
		// reset md5 for the next block
		md5ck.Reset()

		// update etag
		etag.Write(readBuf[:n])

		// add to data block
		md.Data.Blocks = append(md.Data.Blocks, md5str)

		// write data block
		if !s.s3io.IsDataBlockExist(md5str) {
			status, errmsg := s.s3io.WriteDataBlock(readBuf[:n], md5str)
			if status != StatusOK {
				glog.Errorln("failed to create data block", md5str,
					status, errmsg, md.Smd.Bucket, md.Smd.Name)
				return status, errmsg
			}
			glog.V(2).Infoln("created data block", md5str, md.Smd.Bucket, md.Smd.Name)
		} else {
			ddBlocks++
			glog.V(2).Infoln("data block exists", md5str, md.Smd.Bucket, md.Smd.Name)
		}
		totalBlocks++
	}

	glog.V(1).Infoln(md.Smd.Bucket, md.Smd.Name, r.ContentLength,
		"totalBlocks", totalBlocks, "ddBlocks", ddBlocks)

	etagbyte := etag.Sum(nil)
	md.Smd.Etag = hex.EncodeToString(etagbyte)
	return StatusOK, StatusOKStr
}

func (s *S3Server) putObject(r *http.Request, resp *http.Response, bkname string, objname string) {
	// Performance is one critical factor for this dedup layer. Not doing the
	// additional operations here, such as bucket permission check, etc.
	// When creating the metadata object, S3 will do all the checks. If S3
	// rejects the request, no positive refs will be added for the data blocks.
	// gc will clean up them in the background.

	// create the metadata object
	smd := &ObjectSMD{}
	smd.Bucket = bkname
	smd.Name = objname
	smd.Mtime = time.Now().Unix()
	smd.Size = r.ContentLength

	data := &DataBlock{}
	data.BlockSize = ReadBufferSize

	md := &ObjectMD{}
	md.Smd = smd
	md.Data = data

	// read object data and create data blocks
	status, errmsg := s.putObjectData(r, md)
	if status != StatusOK {
		resp.StatusCode = status
		resp.Status = errmsg
		return
	}

	// Marshal ObjectMD to []byte
	mdbyte, err := proto.Marshal(md)
	if err != nil {
		glog.Errorln("failed to Marshal ObjectMD", md, err)
		resp.StatusCode = InternalError
		resp.Status = "failed to Marshal ObjectMD"
		return
	}

	// write out ObjectMD
	status, errmsg = s.s3io.WriteObjectMD(bkname, objname, mdbyte)
	if status != StatusOK {
		resp.StatusCode = status
		resp.Status = errmsg
		return
	}

	glog.V(0).Infoln("successfully created object", bkname, objname, md.Smd.Etag)
}

func (s *S3Server) getOp(r *http.Request, resp *http.Response, bkname string, objname string) {
}

func (s *S3Server) delOp(r *http.Request, resp *http.Response, bkname string, objname string) {
	if s.isBucketOp(objname) {
		resp.StatusCode, resp.Status = s.s3io.DeleteBucket(bkname)
		glog.Infoln("del bucket", r.URL, r.Host, bkname, resp.StatusCode, resp.Status)
	} else {
		s.putObject(r, resp, bkname, objname)
	}
}
