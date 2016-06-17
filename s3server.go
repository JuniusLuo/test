package test

import (
	"errors"
	"flag"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/nu7hatch/gouuid"
)

var ioengine = flag.String("io", "fileio", "the cloud ioengine: fileio or cloudio")
var cmp = flag.Bool("cmp", false, "whether enable compression")

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

type s3Request struct {
	requuid string
	r       *http.Request
}

func (s *S3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bkname, objname := s.getBucketAndObjectName(r)

	w.Header().Set(Server, ServerName)

	if bkname == "" {
		glog.Errorln("InvalidRequest, no bucketname", r.Method, r.URL, r.Host)
		http.Error(w, "InvalidRequest, no bucketname", InvalidRequest)
		return
	}

	// generate uuid as request id
	u, err := uuid.NewV4()
	if err != nil {
		glog.Errorln("failed to generate uuid for", r.Method, bkname, objname)
		http.Error(w, "failed to generate uuid", InternalError)
		return
	}

	req := s3Request{requuid: u.String(), r: r}

	w.Header().Set(RequestID, req.requuid)

	glog.V(2).Infoln(req.requuid, r.Method, r.URL, r.Host, bkname, objname)

	switch r.Method {
	case "POST":
		http.Error(w, NotImplementedStr, NotImplemented)
	case "PUT":
		s.putOp(w, req, bkname, objname)
	case "GET":
		s.getOp(w, req, bkname, objname)
	case "HEAD":
		s.headOp(w, req, bkname, objname)
	case "DELETE":
		s.delOp(w, req, bkname, objname)
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

func (s *S3Server) putOp(w http.ResponseWriter, req s3Request, bkname string, objname string) {
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
		p := NewS3PutObject(req, s.s3io, bkname, objname)
		p.PutObject(w, bkname, objname)
	}
}

type dataBlockReadResult struct {
	partNum int
	blkIdx  int
	blkmd5  string
	buf     []byte
	n       int
	status  int
	errmsg  string
}

type dataPartReadResult struct {
	// data part name
	partName string
	// the data blocks of the data part
	data   *DataBlock
	status int
	errmsg string
}

type objectDataIOReader struct {
	w     http.ResponseWriter
	s3io  CloudIO
	objmd *ObjectMD
	// the read offset
	off      int64
	currPart *DataPart
	// the current cached data block
	currBlock dataBlockReadResult

	// sanity check, whether needs to wait for the outgoing block prefetch
	waitBlock bool
	// channel to wait till the background prefetch complete
	blockChan chan dataBlockReadResult

	// sanity check, whether needs to wait for the outgoing part prefetch
	waitPart bool
	partChan chan dataPartReadResult

	// the reader is possible to involve 2 threads, th1 may be prefetching the block,
	// th2 may be at any step of Read()
	closed chan bool
}

func (d *objectDataIOReader) isLastBlock(partNum int, blkIdx int) bool {
	data := d.objmd.Data
	return partNum == len(data.DataParts)-1 && blkIdx == len(data.DataParts[partNum].Data.Blocks)-1
}

// check to make sure block is valid
func (d *objectDataIOReader) isValidBlock(partNum int, blkIdx int) bool {
	smd := d.objmd.Smd
	// sanity check
	if partNum >= len(d.objmd.Data.DataParts) {
		glog.Errorln("SanityError - read unexist part",
			d.objmd.Uuid, partNum, len(d.objmd.Data.DataParts), smd.Bucket, smd.Name)
		return false
	}

	dataPart := d.objmd.Data.DataParts[partNum]
	// sanity check
	if blkIdx >= len(dataPart.Data.Blocks) {
		glog.Errorln("SanityError - read unexist block", d.objmd.Uuid,
			blkIdx, len(dataPart.Data.Blocks), dataPart.Name, smd.Bucket, smd.Name)
		return false
	}

	return true
}

func (d *objectDataIOReader) readBlock(partNum int, blkIdx int, b []byte) dataBlockReadResult {
	// sanity check
	if !d.isValidBlock(partNum, blkIdx) {
		res := dataBlockReadResult{status: InternalError, errmsg: "read unexist block"}
		return res
	}

	smd := d.objmd.Smd
	dataPart := d.objmd.Data.DataParts[partNum]
	blkmd5 := dataPart.Data.Blocks[blkIdx]

	res := dataBlockReadResult{partNum: partNum, blkIdx: blkIdx, blkmd5: blkmd5, buf: b}

	res.n, res.status, res.errmsg = d.s3io.ReadDataBlockRange(blkmd5, 0, res.buf)

	glog.V(2).Infoln("read block done", d.objmd.Uuid, partNum, blkIdx,
		blkmd5, res.n, res.status, res.errmsg, smd.Bucket, smd.Name)

	if res.status == StatusOK && res.n != int(d.objmd.Data.BlockSize) &&
		!d.isLastBlock(partNum, blkIdx) {
		// read less data, could only happen for the last block
		glog.Errorln("not read full block", d.objmd.Uuid, res.n,
			d.objmd.Data.BlockSize, dataPart.Name, blkIdx, smd.Bucket, smd.Name)
		res.status = InternalError
		res.errmsg = "read less data for a full block"
	}

	return res
}

func (d *objectDataIOReader) prefetchBlock(partNum int, blk int, b []byte) {
	glog.V(5).Infoln("prefetchBlock start", d.objmd.Uuid, partNum, blk, d.objmd.Smd)

	if RandomFI() && !FIRandomSleep() {
		// simulate the connection broken and closeChan() is called
		glog.Errorln("FI at prefetchBlock, close d.closed chan", d.objmd.Uuid, blk, d.objmd.Smd)
		d.closeChan()
	}

	res := d.readBlock(partNum, blk, b)
	select {
	case d.blockChan <- res:
		glog.V(5).Infoln("prefetchBlock sent res to chan done", d.objmd.Uuid, blk, d.objmd.Smd)
	case <-d.closed:
		glog.Errorln("stop prefetchBlock, reader closed", d.objmd.Uuid, blk, d.objmd.Smd)
	case <-time.After(RWTimeOutSecs * time.Second):
		glog.Errorln("stop prefetchBlock, timeout", d.objmd.Uuid, blk, d.objmd.Smd)
	}
}

func (d *objectDataIOReader) prefetchPart(partNum int) {
	glog.V(5).Infoln("prefetchPart start", d.objmd.Uuid, partNum, d.objmd.Smd)

}

func (d *objectDataIOReader) closeChan() {
	glog.V(5).Infoln("closeChan", d.off, d.objmd.Smd.Size)

	// close the "closed" channel, so both prefetchBlock() and Read() can exit
	close(d.closed)
}

func (d *objectDataIOReader) waitPrefetchBlock(partNum int, blkInPart int) error {
	select {
	case nextBlock := <-d.blockChan:
		d.waitBlock = false

		if nextBlock.status != StatusOK {
			glog.Errorln("failed to prefetch block", d.objmd.Uuid,
				nextBlock.partNum, nextBlock.blkIdx, nextBlock.status, nextBlock.errmsg)
			return errors.New(nextBlock.errmsg)
		}

		// sanity check
		if nextBlock.partNum != partNum || nextBlock.blkIdx != blkInPart {
			glog.Errorln("the prefetch block is not the next read block", d.objmd.Uuid,
				nextBlock.partNum, nextBlock.blkIdx, partNum, blkInPart, d.objmd.Smd)
			return errors.New(InternalErrorStr)
		}

		glog.V(5).Infoln("get the prefetch block",
			d.objmd.Uuid, partNum, blkInPart, d.off, d.objmd.Smd)

		// the next block is back, switch the current block to the next block
		oldbuf := d.currBlock.buf
		d.currBlock = nextBlock

		// prefetch the next block if necessary
		if d.currBlock.status == StatusOK && d.off+int64(d.currBlock.n) < d.objmd.Smd.Size {
			if blkInPart == int(d.objmd.Data.MaxBlocks-1) {
				// reach the last block, wait the prefetch part
				err := d.waitPrefetchPart()
				if err != nil {
					return err
				}

				// if not reach the last part, prefetch the next part
				if partNum < len(d.objmd.Data.DataParts)-2 {
					d.waitPart = true
					d.prefetchPart(partNum + 1)
				}

				// prefetch the first block in the next part
				d.waitBlock = true
				go d.prefetchBlock(partNum+1, 0, oldbuf)
			} else {
				// prefetch the next block in the same part
				d.waitBlock = true
				go d.prefetchBlock(partNum, blkInPart+1, oldbuf)
			}
		}
		return nil
	case <-d.closed:
		glog.Errorln("stop Read, reader closed", d.objmd.Uuid, partNum, blkInPart, d.off, d.objmd.Smd)
		return errors.New("connection closed")
	case <-time.After(RWTimeOutSecs * time.Second):
		glog.Errorln("stop Read, timeout", d.objmd.Uuid, partNum, blkInPart, d.off, d.objmd.Smd)
		return errors.New("read timeout")
	}
}

func (d *objectDataIOReader) waitPrefetchPart() error {

	return nil
}

func (d *objectDataIOReader) Read(p []byte) (n int, err error) {
	if d.off >= d.objmd.Smd.Size {
		glog.V(1).Infoln("finish read object data", d.objmd.Smd)
		return 0, io.EOF
	}

	// compute the corresponding data block and offset inside data block
	blockNum := int(d.off / int64(d.objmd.Data.BlockSize))
	blockOff := int(d.off % int64(d.objmd.Data.BlockSize))
	partNum := blockNum / int(d.objmd.Data.MaxBlocks)
	blkIdx := blockNum % int(d.objmd.Data.MaxBlocks)

	// if current block is read out, wait for the next block
	if partNum > d.currBlock.partNum || blkIdx > d.currBlock.blkIdx {
		// sanity check, the prefetch task should be sent already
		if !d.waitBlock {
			glog.Errorln("no prefetch task", blkIdx, d.off, d.objmd.Smd)
			return 0, errors.New("InternalError, no prefetch task")
		}

		glog.V(5).Infoln("wait the prefetch block", blkIdx, d.off, d.objmd.Smd)

		if RandomFI() && !FIRandomSleep() {
			// simulate the connection broken and Close() is called
			// Q: looks the ongoing Read still goes through, d.closed looks not used here.
			glog.Errorln("FI at Read, close d.closed chan", blkIdx, d.off, d.objmd.Smd)
			d.closeChan()
		}

		err := d.waitPrefetchBlock(partNum, blkIdx)
		if err != nil {
			return 0, err
		}
	}

	// check the current block read status
	if d.currBlock.status != StatusOK {
		glog.Errorln("read data block failed", d.objmd.Uuid, partNum, blkIdx,
			d.off, d.currBlock.status, d.currBlock.errmsg, d.objmd.Smd)
		return 0, errors.New(d.currBlock.errmsg)
	}

	// fill data from the current block
	glog.V(2).Infoln("fill data from currBlock",
		blkIdx, blockOff, d.off, d.currBlock.n, d.objmd.Smd)

	endOff := blockOff + len(p)
	if endOff <= d.currBlock.n {
		// currBlock has more data than p
		glog.V(5).Infoln("currBlock has enough data",
			blkIdx, blockOff, endOff, d.off, d.currBlock.n, d.objmd.Smd)

		copy(p, d.currBlock.buf[blockOff:endOff])
		n = len(p)
	} else {
		// p could have more data than the rest in currBlock
		// TODO copy the rest data from the next block
		glog.V(5).Infoln("read the end of currBlock",
			blkIdx, blockOff, endOff, d.off, d.currBlock.n, d.objmd.Smd)

		copy(p, d.currBlock.buf[blockOff:d.currBlock.n])
		n = d.currBlock.n - blockOff
	}

	d.off += int64(n)

	if d.off == d.objmd.Smd.Size {
		return n, io.EOF
	}

	return n, nil
}

func (s *S3Server) getOp(w http.ResponseWriter, req s3Request, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" || objname == BucketListOp {
			body, status, errmsg := s.s3io.GetBucket(bkname)
			if status != StatusOK {
				glog.Errorln("get bucket failed", req.requuid, bkname, objname, status, errmsg)
				http.Error(w, errmsg, status)
				return
			}

			// GetBucket success, copy result to w
			n, err := io.Copy(w, body)
			if err != nil || n == 0 {
				glog.Errorln("get bucket failed", req.requuid, bkname, objname, n, err)
				if n == 0 {
					// n == 0 is not valid, the list would at least have some xml string
					// read and write 0 data, w.Write may not be called in io.Copy
					w.WriteHeader(InternalError)
				}
			} else {
				glog.V(1).Infoln("get bucket success", req.requuid, bkname, objname, n)
			}
		} else {
			glog.Errorln("not support get bucket operation", req.requuid, bkname, objname)
			http.Error(w, NotImplementedStr, NotImplemented)
		}
	} else {
		s.getObjectOp(w, req, bkname, objname)
	}
}

func (s *S3Server) getObjectMD(bkname string, objname string) (objmd *ObjectMD, status int, errmsg string) {
	// object get, read metadata object first
	b, status, errmsg := s.s3io.ReadObjectMD(bkname, objname)
	if status != StatusOK {
		glog.Errorln("failed to ReadObjectMD", bkname, objname, status, errmsg)
		return nil, status, errmsg
	}

	// uncompress ObjectMD bytes
	mdbyte := b
	var err error
	if *cmp {
		mdbyte, err = snappy.Decode(nil, b)
		if err != nil {
			glog.Errorln("failed to uncompress ObjectMD", bkname, objname, err)
			return nil, InternalError, "failed to uncompress ObjectMD"
		}
	}

	objmd = &ObjectMD{}
	err = proto.Unmarshal(mdbyte, objmd)
	if err != nil {
		glog.Errorln("failed to Unmarshal ObjectMD", bkname, objname, err)
		return nil, InternalError, InternalErrorStr
	}

	glog.V(2).Infoln("successfully read object md", bkname, objname, objmd.Smd,
		"compress", len(mdbyte), len(b))
	return objmd, StatusOK, StatusOKStr
}

func (s *S3Server) getObjectOp(w http.ResponseWriter, req s3Request, bkname string, objname string) {
	// object get, read metadata object
	objmd, status, errmsg := s.getObjectMD(bkname, objname)
	if status != StatusOK {
		glog.Errorln("getObjecct failed to get ObjectMD",
			req.requuid, bkname, objname, status, errmsg)
		http.Error(w, errmsg, status)
		return
	}

	w.Header().Set(LastModified, time.Unix(objmd.Smd.Mtime, 0).UTC().Format(time.RFC1123))
	w.Header().Set(ETag, objmd.Smd.Etag)
	w.Header().Set(ContentLength, strconv.FormatInt(objmd.Smd.Size, 10))

	if objmd.Smd.Size == 0 {
		glog.V(1).Infoln("get object success, size 0", req.requuid, bkname, objname)
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
	rd.currBlock = rd.readBlock(0, 0, b)

	// check the first block read status
	if rd.currBlock.status != StatusOK {
		glog.Errorln("read first data block failed",
			req.requuid, objmd.Data.DataParts[0].Data.Blocks[0],
			rd.currBlock.status, rd.currBlock.errmsg, bkname, objname)
		http.Error(w, rd.currBlock.errmsg, rd.currBlock.status)
		return
	}

	// if there are more data to read, start the prefetch task
	if objmd.Smd.Size > int64(objmd.Data.BlockSize) {
		rd.blockChan = make(chan dataBlockReadResult)
		nextbuf := make([]byte, objmd.Data.BlockSize)
		rd.waitBlock = true
		go rd.prefetchBlock(0, 1, nextbuf)
	}

	// if there are more than 2 parts, start the prefetch task
	if len(objmd.Data.DataParts) > 2 {
		rd.partChan = make(chan dataPartReadResult)
		rd.waitPart = true
		go rd.prefetchPart(1)
	}

	n, err := io.Copy(w, rd)
	if err != nil || n == 0 {
		// n == 0 is also an error,
		glog.Errorln("get object failed", req.requuid, bkname, objname, n, err)
		if n == 0 {
			// n == 0 is also an error. if object size is 0, will not reach here.
			// read and write 0 data, w.Write may not be called in io.Copy
			w.WriteHeader(InternalError)
		}
	} else {
		glog.V(1).Infoln("get object success", req.requuid, bkname, objname, n, objmd.Smd)
	}

	// read done, close reader
	rd.closeChan()
}

func (s *S3Server) delOp(w http.ResponseWriter, req s3Request, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" {
			status, errmsg := s.s3io.DeleteBucket(bkname)
			if status != StatusOK {
				glog.Errorln("delete bucket failed", req.requuid, bkname, status, errmsg)
				http.Error(w, errmsg, status)
				return
			}
			glog.Infoln("del bucket success", req.requuid, bkname)
			w.WriteHeader(status)
		} else {
			glog.Errorln("NotImplemented delete bucket operation", req.requuid, bkname, objname)
			http.Error(w, NotImplementedStr, NotImplemented)
		}
	} else {
		s.delObject(w, req, bkname, objname)
	}
}

func (s *S3Server) delObject(w http.ResponseWriter, req s3Request, bkname string, objname string) {
	// read object md

	// log it to the local fs (protected by EBS or the underline storage of VMWare)

	// return success, the background scanner will pick up from log
}

func (s *S3Server) headOp(w http.ResponseWriter, req s3Request, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" {
			status, errmsg := s.s3io.HeadBucket(bkname)
			if status != StatusOK {
				glog.Errorln("failed to head bucket", req.requuid, bkname, status, errmsg)
				http.Error(w, errmsg, status)
				return
			}

			glog.V(2).Infoln("head bucket success", req.requuid, bkname)
			w.WriteHeader(status)
		} else {
			glog.Errorln("Invalid head bucket operation", req.requuid, bkname, objname)
			http.Error(w, "Invalid head bucket operation", InvalidRequest)
		}
	} else {
		s.headObject(w, req, bkname, objname)
	}
}

func (s *S3Server) headObject(w http.ResponseWriter, req s3Request, bkname string, objname string) {
	// get ObjectMD
	objmd, status, errmsg := s.getObjectMD(bkname, objname)
	if status != StatusOK {
		glog.Errorln("headObjecct failed to get ObjectMD",
			req.requuid, bkname, objname, status, errmsg)
		http.Error(w, errmsg, status)
		return
	}

	glog.V(2).Infoln("head object success", req.requuid, objmd.Smd)

	w.Header().Set(LastModified, time.Unix(objmd.Smd.Mtime, 0).UTC().Format(time.RFC1123))
	w.Header().Set(ETag, objmd.Smd.Etag)
	w.WriteHeader(StatusOK)
}
