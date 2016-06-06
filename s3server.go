package test

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"hash"
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

	resp.Header.Set("Date", time.Now().Format(time.RFC1123))

	resp.Write(w)
}

func (s *S3Server) newResponse() *http.Response {
	resp := new(http.Response)
	resp.ProtoMajor = 1
	resp.ProtoMinor = 1
	resp.StatusCode = InvalidRequest
	resp.Status = "not support request"
	resp.Header = make(http.Header)
	resp.Header.Set("Server", ServerName)
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
		// url like /b1/k1 will be split to 3 elements, [ b1 k1].
		// /b1/ also 3 elements [ b1 ].
		// /b1 2 elements [ b1].
		strs := strings.SplitN(r.URL.String(), "/", 3)
		l := len(strs)
		if l == 3 {
			return strs[1], "/" + strs[2]
		} else if l == 2 {
			return strs[1], "/"
		} else {
			return "", ""
		}
	} else {
		// bucket is in r.Host, the whole URL is object name
		return bkname, r.URL.String()
	}
}

func (s *S3Server) isBucketOp(objname string) bool {
	if objname == "" || objname == "/" ||
		strings.HasPrefix(objname, "/?") || strings.HasPrefix(objname, "?") {
		return true
	}
	return false
}

func (s *S3Server) putOp(r *http.Request, resp *http.Response, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" {
			resp.StatusCode, resp.Status = s.s3io.PutBucket(bkname)
			glog.Infoln("put bucket", r.URL, r.Host, bkname, resp.StatusCode, resp.Status)
		} else {
			glog.Errorln("not support put bucket operation", bkname, objname)
		}
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

// if object data < ReadBufferSize
func (s *S3Server) putSmallObjectData(r *http.Request, md *ObjectMD) (status int, errmsg string) {
	readBuf := make([]byte, r.ContentLength)

	// read all data
	n, err := s.readFullBuf(r, readBuf)
	glog.V(4).Infoln("read", n, err, "ContentLength",
		r.ContentLength, md.Smd.Bucket, md.Smd.Name)

	if err != nil {
		if err != io.EOF {
			glog.Errorln("failed to read data from http", err, "ContentLength",
				r.ContentLength, md.Smd.Bucket, md.Smd.Name)
			return InternalError, "failed to read data from http"
		}

		// EOF, check if all contents are readed
		if int64(n) != r.ContentLength {
			glog.Errorln("read", n, "less than ContentLength",
				r.ContentLength, md.Smd.Bucket, md.Smd.Name)
			return InvalidRequest, "data less than ContentLength"
		}
	}

	// compute checksum
	m := md5.New()
	m.Write(readBuf)
	md5byte := m.Sum(nil)
	md5str := hex.EncodeToString(md5byte)
	m.Reset()

	// write data block
	if !s.s3io.IsDataBlockExist(md5str) {
		status, errmsg = s.s3io.WriteDataBlock(readBuf, md5str)
		if status != StatusOK {
			glog.Errorln("failed to create data block",
				md5str, status, errmsg, md.Smd.Bucket, md.Smd.Name)
			return status, errmsg
		}
		glog.V(2).Infoln("create data block", md5str, r.ContentLength)
	} else {
		md.Data.DdBlocks = 1
		glog.V(2).Infoln("data block exists", md5str, r.ContentLength)
	}

	// add to data block
	md.Data.BlockSize = int32(r.ContentLength)
	md.Data.Blocks = append(md.Data.Blocks, md5str)

	// set etag
	md.Smd.Etag = md5str
	return StatusOK, StatusOKStr
}

type writeDataBlockResult struct {
	md5str string // data block md5
	exist  bool   // whether data block exists
	status int
	errmsg string
}

func (s *S3Server) writeOneDataBlock(buf []byte, md5ck hash.Hash, etag hash.Hash, c chan<- writeDataBlockResult) {
	// compute checksum
	md5ck.Write(buf)
	md5byte := md5ck.Sum(nil)
	md5str := hex.EncodeToString(md5byte)
	// reset md5 for the next block
	md5ck.Reset()

	// update etag
	etag.Write(buf)

	res := writeDataBlockResult{md5str, true, StatusOK, StatusOKStr}

	// write data block
	if !s.s3io.IsDataBlockExist(md5str) {
		res.exist = false
		res.status, res.errmsg = s.s3io.WriteDataBlock(buf, md5str)
		glog.V(2).Infoln("create data block", md5str, res.status, len(buf))
	} else {
		glog.V(2).Infoln("data block exists", md5str, len(buf))
	}

	c <- res
}

// read object data and create data blocks.
// this func will update data blocks and etag in ObjectMD
func (s *S3Server) putObjectData(r *http.Request, md *ObjectMD) (status int, errmsg string) {
	if r.ContentLength <= ReadBufferSize {
		return s.putSmallObjectData(r, md)
	}

	readBuf := make([]byte, ReadBufferSize)
	writeBuf := make([]byte, ReadBufferSize)

	md5ck := md5.New()
	etag := md5.New()

	var totalBlocks int64
	// minimal dd blocks
	var ddBlocks int64

	// chan to wait till the previous write completes
	c := make(chan writeDataBlockResult)
	waitWrite := false

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

		if waitWrite {
			// wait data block write done
			res := <-c

			if res.status != StatusOK {
				glog.Errorln("failed to create data block", res.md5str,
					res.status, res.errmsg, md.Smd.Bucket, md.Smd.Name)
				return res.status, res.errmsg
			}

			if res.exist {
				ddBlocks++
			}
			totalBlocks++
			// add to data block
			md.Data.Blocks = append(md.Data.Blocks, res.md5str)
		}

		// write data block
		// switch buffer, readBuf will be used to read the next data block
		tmpbuf := readBuf
		readBuf = writeBuf
		writeBuf = tmpbuf
		go s.writeOneDataBlock(writeBuf[:n], md5ck, etag, c)
		waitWrite = true
	}

	// wait the last write
	if waitWrite {
		// wait data block write done
		res := <-c

		if res.status != StatusOK {
			glog.Errorln("failed to create data block", res.md5str,
				res.status, res.errmsg, md.Smd.Bucket, md.Smd.Name)
			return res.status, res.errmsg
		}

		if res.exist {
			ddBlocks++
		}
		totalBlocks++
		// add to data block
		md.Data.Blocks = append(md.Data.Blocks, res.md5str)
	}

	glog.V(1).Infoln(md.Smd.Bucket, md.Smd.Name, r.ContentLength,
		"totalBlocks", totalBlocks, "ddBlocks", ddBlocks)

	etagbyte := etag.Sum(nil)
	md.Smd.Etag = hex.EncodeToString(etagbyte)
	md.Data.DdBlocks = ddBlocks
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

	resp.Header.Set("ETag", md.Smd.Etag)
	resp.ContentLength = 0
	resp.StatusCode = StatusOK
	resp.Status = StatusOKStr
}

type objectDataIOReader struct {
	s3io  CloudIO
	objmd *ObjectMD
	off   int64
}

func (d *objectDataIOReader) Close() error {
	return nil
}

func (d *objectDataIOReader) Read(p []byte) (n int, err error) {
	if d.off >= d.objmd.Smd.Size {
		glog.V(4).Infoln("finish read object", d.objmd.Smd)
		return 0, io.EOF
	}

	// compute the corresponding data block and offset inside data block
	idx := d.off / int64(d.objmd.Data.BlockSize)
	blockOff := d.off % int64(d.objmd.Data.BlockSize)

	// read the data block
	n, status, errmsg := d.s3io.ReadDataBlockRange(d.objmd.Data.Blocks[idx], blockOff, p)

	if status != StatusOK {
		glog.Errorln("failed to read data block", d.objmd.Data.Blocks[idx], blockOff, d.off, d.objmd.Smd)
		return n, errors.New(errmsg)
	}

	glog.V(4).Infoln("read data block", idx, d.objmd.Data.Blocks[idx], blockOff, n,
		"object off", d.off, d.objmd.Smd)

	d.off += int64(n)

	if d.off == d.objmd.Smd.Size {
		return n, io.EOF
	}

	return n, nil
}

func (s *S3Server) getOp(r *http.Request, resp *http.Response, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" || objname == BucketListOp {
			s.s3io.GetBucket(bkname, resp)
		} else {
			glog.Errorln("not support get bucket operation", bkname, objname)
		}
	} else {
		// read metadata object first
		b, status, errmsg := s.s3io.ReadObjectMD(bkname, objname)
		if status != StatusOK {
			glog.Errorln("failed to ReadObjectMD", bkname, objname, status, errmsg)
			resp.StatusCode = status
			resp.Status = errmsg
			return
		}

		objmd := &ObjectMD{}
		err := proto.Unmarshal(b, objmd)
		if err != nil {
			glog.Errorln("failed to Unmarshal ObjectMD", bkname, objname, err)
			resp.StatusCode = InternalError
			resp.Status = InternalErrorStr
			return
		}

		// read the corresponding data blocks
		rd := new(objectDataIOReader)
		rd.s3io = s.s3io
		rd.objmd = objmd

		resp.Body = rd
		resp.StatusCode = StatusOK
		resp.Status = StatusOKStr

		glog.V(1).Infoln("successfully read object md", bkname, objname)
	}
}

func (s *S3Server) delOp(r *http.Request, resp *http.Response, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" {
			resp.StatusCode, resp.Status = s.s3io.DeleteBucket(bkname)
			glog.Infoln("del bucket", r.URL, r.Host, bkname, resp.StatusCode, resp.Status)
		} else {
			glog.Errorln("not support delete bucket operation", bkname, objname)
		}
	} else {
		s.putObject(r, resp, bkname, objname)
	}
}
