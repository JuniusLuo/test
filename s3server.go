package test

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"flag"
	"hash"
	"io"
	"net/http"
	"runtime"
	"strconv"
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
	bkname, objname := s.getBucketAndObjectName(r)
	glog.V(2).Infoln(r.Method, r.URL, r.Host, bkname, objname)

	w.Header().Set(Server, ServerName)

	if bkname == "" {
		glog.Errorln("InvalidRequest, no bucketname", r.Method, r.URL, r.Host)
		http.Error(w, "InvalidRequest, no bucketname", InvalidRequest)
		return
	}

	switch r.Method {
	case "POST":
		http.Error(w, NotImplementedStr, NotImplemented)
	case "PUT":
		s.putOp(w, r, bkname, objname)
	case "GET":
		s.getOp(w, r, bkname, objname)
	case "HEAD":
		s.headOp(w, r, bkname, objname)
	case "DELETE":
		s.delOp(w, r, bkname, objname)
	case "OPTIONS":
		http.Error(w, NotImplementedStr, NotImplemented)
	default:
		glog.Errorln("unsupported request", r.Method, r.URL)
		http.Error(w, "Invalid method", InvalidRequest)
	}
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

	glog.Errorln("invalid url", host)
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

func (s *S3Server) putOp(w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" {
			status, errmsg := s.s3io.PutBucket(bkname)
			if status != StatusOK {
				glog.Errorln("put bucket failed", bkname, status, errmsg)
				http.Error(w, errmsg, status)
				return
			}
			glog.Infoln("put bucket success", bkname)
			w.WriteHeader(status)
		} else {
			glog.Errorln("NotImplemented put bucket operation", bkname, objname)
			http.Error(w, NotImplementedStr, NotImplemented)
		}
	} else {
		s.putObject(w, r, bkname, objname)
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
	md.Data.BlockSize = ReadBufferSize
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

func (s *S3Server) writeOneDataBlock(buf []byte, md5ck hash.Hash, etag hash.Hash,
	md *ObjectMD, c chan<- writeDataBlockResult, quit <-chan bool) {
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

	select {
	case c <- res:
		glog.V(5).Infoln("sent writeDataBlockResult", md5str, md.Smd)
	case <-quit:
		glog.V(5).Infoln("write data block quit", md.Smd)
	case <-time.After(RWTimeOutSecs * time.Second):
		glog.Errorln("write data block timeout", md5str, md.Smd,
			"NumGoroutine", runtime.NumGoroutine())
	}
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
	quit := make(chan bool)
	waitWrite := false

	var rlen int64
	for rlen < r.ContentLength {
		// read one block
		n, err := s.readFullBuf(r, readBuf)
		rlen += int64(n)
		glog.V(4).Infoln("read", n, err, "total readed len", rlen,
			"specified read len", r.ContentLength, md.Smd.Bucket, md.Smd.Name)

		if RandomFI() && !FIRandomSleep() {
			glog.Errorln("FI at putObjectData", rlen, r.ContentLength, md.Smd,
				"NumGoroutine", runtime.NumGoroutine())
			if RandomFI() {
				// test writer timeout to exit goroutine
				return InternalError, "exit early to test chan timeout"
			}
			// test quit writer
			err = io.EOF
		}

		if err != nil {
			if err != io.EOF {
				glog.Errorln("failed to read data from http", err, "readed len", rlen,
					"ContentLength", r.ContentLength, md.Smd.Bucket, md.Smd.Name)
				if waitWrite {
					glog.V(5).Infoln("notify writer to quit", md.Smd)
					quit <- true
				}
				return InternalError, "failed to read data from http"
			}

			// EOF, check if all contents are readed
			if rlen != r.ContentLength {
				glog.Errorln("read", rlen, "less than ContentLength",
					r.ContentLength, md.Smd.Bucket, md.Smd.Name)
				if waitWrite {
					glog.V(5).Infoln("notify writer to quit", md.Smd)
					quit <- true
				}
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
		waitWrite = true
		// Note: should we switch to a single routine, which loops to write data
		// block. and here invoke the routine via chan? assume go internally has
		// like a queue for all routines, and one thread per core to schedule them.
		// Sounds no big difference? an old routine + chan vs a new routine.
		go s.writeOneDataBlock(writeBuf[:n], md5ck, etag, md, c, quit)
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

func (s *S3Server) putObject(w http.ResponseWriter, r *http.Request, bkname string, objname string) {
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
		glog.Errorln("put object failed", bkname, objname, status, errmsg)
		http.Error(w, errmsg, status)
		return
	}

	// Marshal ObjectMD to []byte
	mdbyte, err := proto.Marshal(md)
	if err != nil {
		glog.Errorln("failed to marshal ObjectMD", bkname, objname, md, err)
		http.Error(w, "failed to marshal ObjectMD", InternalError)
		return
	}

	// write out ObjectMD
	status, errmsg = s.s3io.WriteObjectMD(bkname, objname, mdbyte)
	if status != StatusOK {
		glog.Errorln("failed to write ObjectMD", bkname, objname, status, errmsg)
		http.Error(w, errmsg, status)
		return
	}

	glog.V(0).Infoln("create object success", bkname, objname, md.Smd.Etag)

	w.Header().Set(ETag, md.Smd.Etag)
	w.WriteHeader(status)
}

type dataBlockReadResult struct {
	blkIdx int
	buf    []byte
	n      int
	status int
	errmsg string
}

type objectDataIOReader struct {
	w     http.ResponseWriter
	s3io  CloudIO
	objmd *ObjectMD
	off   int64
	// the current cached data block
	currBlock dataBlockReadResult
	// sanity check, whether needs to wait for the outgoing prefetch
	waitBlock bool
	// channel to wait till the background prefetch complete
	c chan dataBlockReadResult
	// the reader is possible to involve 2 threads, th1 may be prefetching the block,
	// th2 may be at any step of Read()
	closed chan bool
	// internal for FI usage only, to avoid close the chan multiple times
	closeCalled bool
}

func (d *objectDataIOReader) readBlock(blk int, b []byte) dataBlockReadResult {
	res := dataBlockReadResult{blkIdx: blk, buf: b}

	// sanity check
	if blk >= len(d.objmd.Data.Blocks) {
		glog.Errorln("no more block to read", blk, d.objmd)
		res.status = InternalError
		res.errmsg = "no more block to read"
		return res
	}

	res.n, res.status, res.errmsg =
		d.s3io.ReadDataBlockRange(d.objmd.Data.Blocks[blk], 0, res.buf)

	glog.V(2).Infoln("read block done", blk, d.objmd.Data.Blocks[blk],
		res.n, res.status, res.errmsg, d.objmd.Smd)

	if res.status == StatusOK && res.n != int(d.objmd.Data.BlockSize) &&
		res.blkIdx != len(d.objmd.Data.Blocks)-1 {
		// read less data, could only happen for the last block
		glog.Errorln("not read full block", res.n, blk, d.objmd)
		res.status = InternalError
		res.errmsg = "read less data for a full block"
	}

	return res
}

func (d *objectDataIOReader) prefetchBlock(blk int, b []byte) {
	glog.V(5).Infoln("prefetchBlock start", blk, d.objmd.Smd)

	if RandomFI() && !FIRandomSleep() {
		// simulate the connection broken and closeChan() is called
		glog.Errorln("FI at prefetchBlock, close d.closed chan", blk, d.objmd.Smd)
		d.closeChan()
	}

	res := d.readBlock(blk, b)
	select {
	case d.c <- res:
		glog.V(5).Infoln("prefetchBlock sent res to chan done", blk, d.objmd.Smd)
	case <-d.closed:
		glog.Errorln("stop prefetchBlock, reader closed", blk, d.objmd.Smd)
	case <-time.After(RWTimeOutSecs * time.Second):
		glog.Errorln("stop prefetchBlock, timeout", blk, d.objmd.Smd)
	}
}

func (d *objectDataIOReader) closeChan() {
	if FIEnabled() && d.closeCalled {
		return
	}

	glog.V(5).Infoln("closeChan", d.off, d.objmd.Smd.Size)

	// close the "closed" channel, so both prefetchBlock() and Read() can exit
	close(d.closed)
	d.closeCalled = true
}

func (d *objectDataIOReader) Read(p []byte) (n int, err error) {
	if d.off >= d.objmd.Smd.Size {
		glog.V(1).Infoln("finish read object data", d.objmd.Smd)
		return 0, io.EOF
	}

	// compute the corresponding data block and offset inside data block
	idx := int(d.off / int64(d.objmd.Data.BlockSize))
	blockOff := int(d.off % int64(d.objmd.Data.BlockSize))

	if idx < d.currBlock.blkIdx {
		// sanity check, this should not happen
		glog.Errorln("read the previous data again?",
			d.off, idx, d.currBlock.blkIdx, d.objmd.Smd)
		return 0, errors.New("Invalid read request, read previous data again")
	}

	if idx > d.currBlock.blkIdx {
		// sanity check, the prefetch task should be sent already
		if !d.waitBlock {
			glog.Errorln("no prefetch task", idx, d.off, d.objmd.Smd)
			return 0, errors.New("InternalError, no prefetch task")
		}

		glog.V(5).Infoln("wait the prefetch block", idx, d.off, d.objmd.Smd)

		if RandomFI() && !FIRandomSleep() {
			// simulate the connection broken and Close() is called
			// Q: looks the ongoing Read still goes through, d.closed looks not used here.
			glog.Errorln("FI at Read, close d.closed chan", idx, d.off, d.objmd.Smd)
			d.closeChan()
		}

		// current block is read out, wait for the next block
		select {
		case nextBlock := <-d.c:
			d.waitBlock = false

			glog.V(5).Infoln("get the prefetch block", idx, d.off, d.objmd.Smd)

			// the next block is back, switch the current block to the next block
			oldbuf := d.currBlock.buf
			d.currBlock = nextBlock

			// prefetch the next block if necessary
			if d.currBlock.status == StatusOK && d.off+int64(d.currBlock.n) < d.objmd.Smd.Size {
				d.waitBlock = true
				go d.prefetchBlock(d.currBlock.blkIdx+1, oldbuf)
			}
		case <-d.closed:
			glog.Errorln("stop Read, reader closed", idx, d.off, d.objmd.Smd)
			return 0, errors.New("connection closed")
		case <-time.After(RWTimeOutSecs * time.Second):
			glog.Errorln("stop Read, timeout", idx, d.off, d.objmd.Smd)
			return 0, errors.New("read timeout")
		}
	}

	// check the current block read status
	if d.currBlock.status != StatusOK {
		glog.Errorln("read data block failed", idx, d.objmd.Data.Blocks[idx],
			d.off, d.currBlock.status, d.currBlock.errmsg, d.objmd.Smd)
		return 0, errors.New(d.currBlock.errmsg)
	}

	// fill data from the current block
	glog.V(2).Infoln("fill data from currBlock",
		idx, blockOff, d.off, d.currBlock.n, d.objmd.Smd)

	endOff := blockOff + len(p)
	if endOff <= d.currBlock.n {
		// currBlock has more data than p
		glog.V(5).Infoln("currBlock has enough data",
			idx, blockOff, endOff, d.off, d.currBlock.n, d.objmd.Smd)

		copy(p, d.currBlock.buf[blockOff:endOff])
		n = len(p)
	} else {
		// p could have more data than the rest in currBlock
		// TODO copy the rest data from the next block
		glog.V(5).Infoln("read the end of currBlock",
			idx, blockOff, endOff, d.off, d.currBlock.n, d.objmd.Smd)

		copy(p, d.currBlock.buf[blockOff:d.currBlock.n])
		n = d.currBlock.n - blockOff
	}

	d.off += int64(n)

	if d.off == d.objmd.Smd.Size {
		return n, io.EOF
	}

	return n, nil
}

func (s *S3Server) getOp(w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" || objname == BucketListOp {
			body, status, errmsg := s.s3io.GetBucket(bkname)
			if status != StatusOK {
				glog.Errorln("get bucket failed", bkname, objname, status, errmsg)
				http.Error(w, errmsg, status)
				return
			}

			// GetBucket success, copy result to w
			n, err := io.Copy(w, body)
			if err != nil || n == 0 {
				glog.Errorln("get bucket failed", bkname, objname, n, err)
				if n == 0 {
					// n == 0 is not valid, the list would at least have some xml string
					// read and write 0 data, w.Write may not be called in io.Copy
					w.WriteHeader(InternalError)
				}
			} else {
				glog.V(1).Infoln("get bucket success", bkname, objname, n)
			}
		} else {
			glog.Errorln("not support get bucket operation", bkname, objname)
			http.Error(w, NotImplementedStr, NotImplemented)
		}
	} else {
		s.getObjectOp(w, r, bkname, objname)
	}
}

func (s *S3Server) getObjectMD(bkname string, objname string) (objmd *ObjectMD, status int, errmsg string) {
	// object get, read metadata object first
	b, status, errmsg := s.s3io.ReadObjectMD(bkname, objname)
	if status != StatusOK {
		glog.Errorln("failed to ReadObjectMD", bkname, objname, status, errmsg)
		return nil, status, errmsg
	}

	objmd = &ObjectMD{}
	err := proto.Unmarshal(b, objmd)
	if err != nil {
		glog.Errorln("failed to Unmarshal ObjectMD", bkname, objname, err)
		return nil, InternalError, InternalErrorStr
	}

	glog.V(2).Infoln("successfully read object md", bkname, objname, objmd.Smd)
	return objmd, StatusOK, StatusOKStr
}

func (s *S3Server) getObjectOp(w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	// object get, read metadata object
	objmd, status, errmsg := s.getObjectMD(bkname, objname)
	if status != StatusOK {
		glog.Errorln("getObjecct failed to get ObjectMD",
			bkname, objname, status, errmsg)
		http.Error(w, errmsg, status)
		return
	}

	w.Header().Set(LastModified, time.Unix(objmd.Smd.Mtime, 0).UTC().Format(time.RFC1123))
	w.Header().Set(ETag, objmd.Smd.Etag)
	w.Header().Set(ContentLength, strconv.FormatInt(objmd.Smd.Size, 10))

	if objmd.Smd.Size == 0 {
		glog.V(1).Infoln("get object success, size 0", bkname, objname)
		w.WriteHeader(status)
		return
	}

	// construct Body reader
	// read the corresponding data blocks
	rd := new(objectDataIOReader)
	rd.w = w
	rd.s3io = s.s3io
	rd.objmd = objmd
	rd.closed = make(chan bool)

	// synchronously read the first block
	b := make([]byte, objmd.Data.BlockSize)
	rd.currBlock = rd.readBlock(0, b)

	// check the first block read status
	if rd.currBlock.status != StatusOK {
		glog.Errorln("read first data block failed", objmd.Data.Blocks[0],
			rd.currBlock.status, rd.currBlock.errmsg, bkname, objname)
		http.Error(w, rd.currBlock.errmsg, rd.currBlock.status)
		return
	}

	// if there are more data to read, start the prefetch task
	if objmd.Smd.Size > int64(objmd.Data.BlockSize) {
		rd.c = make(chan dataBlockReadResult)
		nextbuf := make([]byte, objmd.Data.BlockSize)
		rd.waitBlock = true
		go rd.prefetchBlock(1, nextbuf)
	}

	n, err := io.Copy(w, rd)
	if err != nil || n == 0 {
		// n == 0 is also an error,
		glog.Errorln("get object failed", bkname, objname, n, err)
		if n == 0 {
			// n == 0 is also an error. if object size is 0, will not reach here.
			// read and write 0 data, w.Write may not be called in io.Copy
			w.WriteHeader(InternalError)
		}
	} else {
		glog.V(1).Infoln("get object success", bkname, objname, n, objmd.Smd)
	}

	// read done, close reader
	rd.closeChan()
}

func (s *S3Server) delOp(w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" {
			status, errmsg := s.s3io.DeleteBucket(bkname)
			if status != StatusOK {
				glog.Errorln("delete bucket failed", bkname, status, errmsg)
				http.Error(w, errmsg, status)
				return
			}
			glog.Infoln("del bucket success", bkname)
			w.WriteHeader(status)
		} else {
			glog.Errorln("NotImplemented delete bucket operation", bkname, objname)
			http.Error(w, NotImplementedStr, NotImplemented)
		}
	} else {
		s.putObject(w, r, bkname, objname)
	}
}

func (s *S3Server) deleteObject(w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	// read object md

	// log it to the local fs (protected by EBS or the underline storage of VMWare)

	// return success, the background scanner will pick up from log
}

func (s *S3Server) headOp(w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" {
			status, errmsg := s.s3io.HeadBucket(bkname)
			if status != StatusOK {
				glog.Errorln("failed to head bucket", bkname, status, errmsg)
				http.Error(w, errmsg, status)
				return
			}

			glog.V(2).Infoln("head bucket success", bkname)
			w.WriteHeader(status)
		} else {
			glog.Errorln("Invalid head bucket operation", bkname, objname)
			http.Error(w, "Invalid head bucket operation", InvalidRequest)
		}
	} else {
		s.headObject(w, r, bkname, objname)
	}
}

func (s *S3Server) headObject(w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	// get ObjectMD
	objmd, status, errmsg := s.getObjectMD(bkname, objname)
	if status != StatusOK {
		glog.Errorln("headObjecct failed to get ObjectMD",
			bkname, objname, status, errmsg)
		http.Error(w, errmsg, status)
		return
	}

	glog.V(2).Infoln("head object success", objmd.Smd)

	w.Header().Set(LastModified, time.Unix(objmd.Smd.Mtime, 0).UTC().Format(time.RFC1123))
	w.Header().Set(ETag, objmd.Smd.Etag)
	w.WriteHeader(StatusOK)
}
