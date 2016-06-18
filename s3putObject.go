package test

import (
	"crypto/md5"
	"encoding/hex"
	"hash"
	"io"
	"net/http"
	"runtime"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
)

// S3PutObject is the class to handle object creation
type S3PutObject struct {
	req     s3Request
	s3io    CloudIO
	bkname  string
	objname string

	// internal variables

	// ObjectMD
	md *ObjectMD
	// statistics
	totalBlocks int64
	ddBlocks    int64

	// chan to wait for the data block write done
	blockChan chan writeDataBlockResult
	// chan to wait for the data part write done
	partChan chan writeDataPartResult
	// chan to notify the block/part routines to quit
	quitChan chan bool
}

// NewS3PutObject creates a new S3PutObject instance
func NewS3PutObject(req s3Request, s3io CloudIO, bkname string, objname string) *S3PutObject {
	s := new(S3PutObject)
	s.req = req
	s.s3io = s3io
	s.bkname = bkname
	s.objname = objname
	return s
}

func (s *S3PutObject) addPart(block *DataBlock, partNum int) {
	part := &DataPart{}
	part.Name = GenPartName(s.md.Uuid, partNum)
	part.Data = block

	s.md.Data.DataParts = append(s.md.Data.DataParts, part)
}

func (s *S3PutObject) readFullBuf(readBuf []byte) (n int, err error) {
	readZero := false
	var rlen int
	for rlen < len(readBuf) {
		n, err = s.req.r.Body.Read(readBuf[rlen:])
		rlen += n

		if err != nil {
			return rlen, err
		}

		if n == 0 && err == nil {
			if readZero {
				glog.Errorln("read 0 bytes from http with nil error twice",
					s.req.requuid, s.req.r.URL, s.req.r.Host)
				return rlen, err
			}
			// allow read 0 byte with nil error once
			readZero = true
		}
	}
	return rlen, err
}

// if object data < DataBlockSize
func (s *S3PutObject) putSmallObjectData() (status int, errmsg string) {
	r := s.req.r

	readBuf := make([]byte, r.ContentLength)

	// read all data
	n, err := s.readFullBuf(readBuf)
	glog.V(4).Infoln(s.req.requuid, "read", n, err, "ContentLength",
		r.ContentLength, s.bkname, s.objname)

	if err != nil {
		if err != io.EOF {
			glog.Errorln("failed to read data from http", s.req.requuid, err, "ContentLength",
				r.ContentLength, s.bkname, s.objname)
			return InternalError, "failed to read data from http"
		}

		// EOF, check if all contents are readed
		if int64(n) != r.ContentLength {
			glog.Errorln(s.req.requuid, "read", n, "less than ContentLength",
				r.ContentLength, s.bkname, s.objname)
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
				s.req.requuid, md5str, status, errmsg, s.bkname, s.objname)
			return status, errmsg
		}
		glog.V(2).Infoln("create data block", s.req.requuid, md5str, r.ContentLength)
	} else {
		s.md.Data.DdBlocks = 1
		glog.V(2).Infoln("data block exists", s.req.requuid, md5str, r.ContentLength)
	}

	block := &DataBlock{}
	block.Blocks = append(block.Blocks, md5str)

	// add to data block
	s.addPart(block, 0)

	// set etag
	s.md.Smd.Size = int64(n)
	s.md.Smd.Etag = md5str
	return StatusOK, StatusOKStr
}

type writeDataBlockResult struct {
	md5str string // data block md5
	exist  bool   // whether data block exists
	status int
	errmsg string
}

func (s *S3PutObject) writeOneDataBlock(buf []byte, md5ck hash.Hash, etag hash.Hash) {
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
		glog.V(2).Infoln("create data block", md5str, res.status, len(buf), s.bkname, s.objname)
	} else {
		glog.V(2).Infoln("data block exists", md5str, len(buf), s.bkname, s.objname)
	}

	select {
	case s.blockChan <- res:
		glog.V(5).Infoln("sent writeDataBlockResult", md5str, s.bkname, s.objname)
	case <-s.quitChan:
		glog.V(5).Infoln("write data block quit", s.bkname, s.objname)
	case <-time.After(RWTimeOutSecs * time.Second):
		glog.Errorln("send writeDataBlockResult timeout", md5str, s.bkname, s.objname,
			"NumGoroutine", runtime.NumGoroutine())
	}
}

type writeDataPartResult struct {
	partName string
	partNum  int
	status   int
	errmsg   string
}

// create the part object for the block
func (s *S3PutObject) writeDataPart(block *DataBlock, partName string, partNum int) {
	glog.V(2).Infoln("start creating data part", s.req.requuid, partName, s.bkname, s.objname)

	res := writeDataPartResult{partName, partNum, StatusOK, StatusOKStr}

	b, err := proto.Marshal(block)
	if err == nil {
		res.status, res.errmsg = s.s3io.WriteDataPart(s.bkname, partName, b)
		glog.V(2).Infoln("create data part done", s.req.requuid,
			partName, s.bkname, s.objname, res.status, res.errmsg)
	} else {
		glog.Errorln("failed to Marshal DataBlock",
			s.req.requuid, partName, s.bkname, s.objname, err)
		res.status = InternalError
		res.errmsg = "failed to Marshal DataBlock"
	}

	select {
	case s.partChan <- res:
		glog.V(5).Infoln("sent writeDataPartResult",
			s.req.requuid, partName, s.bkname, s.objname)
	case <-s.quitChan:
		glog.V(5).Infoln("write data part quit", s.req.requuid, partName, s.bkname, s.objname)
	case <-time.After(RWTimeOutSecs * time.Second):
		glog.Errorln("send writeDataPartResult timeout",
			s.req.requuid, partName, s.bkname, s.objname)
	}

}

func (s *S3PutObject) waitAndAddDataPart(partNum int) (status int, errmsg string) {
	// third or later block part, wait for the previous part result
	glog.V(5).Infoln("wait for part write result", s.req.requuid, s.bkname, s.objname)

	partres := <-s.partChan
	if partres.status != StatusOK {
		glog.Errorln("failed to write block part", s.req.requuid, s.bkname, s.objname, partres)
		return partres.status, partres.errmsg
	}

	// sanity check
	if partres.partNum != partNum {
		glog.Errorln("partNum not match", s.req.requuid, s.bkname, s.objname, partres.partNum, partNum)
		return InternalError, InternalErrorStr
	}

	glog.V(2).Infoln("write part success", partres.partName, s.bkname, s.objname)

	part := &DataPart{}
	part.Name = partres.partName

	// add the part to ObjectMD
	s.md.Data.DataParts = append(s.md.Data.DataParts, part)

	return StatusOK, StatusOKStr
}

func (s *S3PutObject) waitLastWrite(waitWrite bool, block *DataBlock, waitPart bool, partNum int) (status int, errmsg string) {
	// wait the last write
	if waitWrite {
		glog.V(5).Infoln("wait the last block write", s.req.requuid, s.bkname, s.objname)

		// wait data block write done
		res := <-s.blockChan

		if res.status != StatusOK {
			glog.Errorln("failed to write the last block", s.req.requuid, res.md5str,
				res.status, res.errmsg, s.bkname, s.objname)
			return res.status, res.errmsg
		}

		glog.V(5).Infoln("last block write success",
			s.req.requuid, res.md5str, s.bkname, s.objname)

		if res.exist {
			s.ddBlocks++
		}
		s.totalBlocks++
		// add to data block
		block.Blocks = append(block.Blocks, res.md5str)
	}

	// wait the last part
	if !waitPart {
		// no split happened, total parts <= 2
		// sanity check, partNum should be 0
		if partNum != 0 && partNum != 1 {
			glog.Errorln("no part split happened, but partNum is not 0 or 1",
				s.req.requuid, partNum, s.bkname, s.objname)
			return InternalError, InternalErrorStr
		}

		glog.V(5).Infoln("add the last part",
			s.req.requuid, partNum, len(block.Blocks), s.bkname, s.objname)

		s.addPart(block, partNum)
	} else {
		// wait the data part
		status, errmsg = s.waitAndAddDataPart(partNum - 1)
		if status != StatusOK {
			return status, errmsg
		}

		glog.V(2).Infoln("check the last part",
			s.req.requuid, partNum, len(block.Blocks), s.bkname, s.objname)

		// add the last part
		s.addPart(block, partNum)
	}

	return StatusOK, StatusOKStr
}

// read object data and create data blocks.
// this func will update data blocks and etag in ObjectMD
func (s *S3PutObject) putObjectData() (status int, errmsg string) {
	r := s.req.r
	if r.ContentLength <= DataBlockSize && r.ContentLength != -1 {
		return s.putSmallObjectData()
	}

	// the first block is the same with the one in ObjectMD
	block := &DataBlock{}
	partNum := 0
	waitPart := false
	s.partChan = make(chan writeDataPartResult)

	readBuf := make([]byte, DataBlockSize)
	writeBuf := make([]byte, DataBlockSize)

	md5ck := md5.New()
	etag := md5.New()

	// chan to wait till the previous write completes
	waitWrite := false
	s.blockChan = make(chan writeDataBlockResult)

	s.quitChan = make(chan bool)
	// close at the end, to ensure all routines exit
	defer close(s.quitChan)

	var rlen int64
	for rlen < r.ContentLength || r.ContentLength == -1 {
		// read one block
		n, err := s.readFullBuf(readBuf)
		rlen += int64(n)
		glog.V(4).Infoln(s.req.requuid, "read", n, err, "total readed len", rlen,
			"specified read len", r.ContentLength, s.bkname, s.objname)

		if RandomFI() && !FIRandomSleep() {
			glog.Errorln("FI at putObjectData", s.req.requuid, rlen, r.ContentLength,
				s.bkname, s.objname, "NumGoroutine", runtime.NumGoroutine())
			if RandomFI() {
				// test writer timeout to exit goroutine
				return InternalError, "exit early to test chan timeout"
			}
			// test quit writer
			err = io.EOF
		}

		if err != nil {
			if err != io.EOF {
				glog.Errorln("failed to read data from http", s.req.requuid, err, "readed len",
					rlen, "ContentLength", r.ContentLength, s.bkname, s.objname)
				return InternalError, "failed to read data from http"
			}

			// EOF, check if all contents are readed
			if rlen != r.ContentLength && r.ContentLength != -1 {
				glog.Errorln(s.req.requuid, "read", rlen, "less than ContentLength",
					r.ContentLength, s.bkname, s.objname)
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
			res := <-s.blockChan
			if res.status != StatusOK {
				glog.Errorln("failed to create data block", s.req.requuid, res.md5str,
					res.status, res.errmsg, s.bkname, s.objname)
				return res.status, res.errmsg
			}

			glog.V(5).Infoln("data block write success", s.req.requuid, res.md5str, s.bkname, s.objname)

			if res.exist {
				s.ddBlocks++
			}
			s.totalBlocks++

			// add to data block
			block.Blocks = append(block.Blocks, res.md5str)
		}

		// write data block
		// switch buffer, readBuf will be used to read the next data block
		tmpbuf := readBuf
		readBuf = writeBuf
		writeBuf = tmpbuf
		// Note: should we switch to a single routine, which loops to write data
		// block. and here invoke the routine via chan? assume go internally has
		// like a queue for all routines, and one thread per core to schedule them.
		// Sounds no big difference? an old routine + chan vs a new routine.
		waitWrite = true
		go s.writeOneDataBlock(writeBuf[:n], md5ck, etag)

		// check whether need to split data to parts
		if len(block.Blocks) >= MaxDataBlocks {
			// object has lots of blocks, split to parts
			// first block part will be stored in ObjectMD
			glog.V(2).Infoln("split data blocks to parts",
				s.req.requuid, partNum, len(block.Blocks), s.bkname, s.objname)

			if partNum == 0 {
				s.addPart(block, 0)
			} else if partNum >= 1 {
				if partNum > 1 {
					// wait the data part
					status, errmsg = s.waitAndAddDataPart(partNum - 1)
					if status != StatusOK {
						return status, errmsg
					}
				}

				glog.V(2).Infoln("write data part", s.md.Uuid, partNum, s.bkname, s.objname)

				// write data part
				partName := GenPartName(s.md.Uuid, partNum)
				waitPart = true
				go s.writeDataPart(block, partName, partNum)
			}

			// increase part number
			partNum++
			// create a new DataBlock
			block = &DataBlock{}
		}
	}

	// wait the possible outgoing block/part write
	status, errmsg = s.waitLastWrite(waitWrite, block, waitPart, partNum)
	if status != StatusOK {
		return status, errmsg
	}

	glog.V(1).Infoln(s.req.requuid, s.bkname, s.objname, r.ContentLength, rlen,
		"totalBlocks", s.totalBlocks, "ddBlocks", s.ddBlocks)

	etagbyte := etag.Sum(nil)
	s.md.Smd.Etag = hex.EncodeToString(etagbyte)
	s.md.Smd.Size = rlen
	s.md.Data.DdBlocks = s.ddBlocks
	return StatusOK, StatusOKStr
}

// PutObject creates the object's data and metadata objects in s3
func (s *S3PutObject) PutObject(w http.ResponseWriter, bkname string, objname string) {
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

	data := &ObjectData{}
	data.BlockSize = DataBlockSize
	data.MaxBlocks = MaxDataBlocks

	s.md = &ObjectMD{}
	s.md.Uuid = s.req.requuid
	s.md.Smd = smd
	s.md.Data = data

	// read object data and create data blocks
	status, errmsg := s.putObjectData()
	if status != StatusOK {
		glog.Errorln("put object failed", s.req.requuid, bkname, objname, status, errmsg)
		http.Error(w, errmsg, status)
		return
	}

	// Marshal ObjectMD to []byte
	mdbyte, err := proto.Marshal(s.md)
	if err != nil {
		glog.Errorln("failed to marshal ObjectMD", s.req.requuid, bkname, objname, s.md, err)
		http.Error(w, "failed to marshal ObjectMD", InternalError)
		return
	}

	// compress ObjectMD bytes
	b := mdbyte
	if *cmp {
		// looks compression is not useful for ObjectMD.
		// tried like /usr/local/bin/docker, 9MB, mdbyte is 2519, compress to 2524
		b = snappy.Encode(nil, mdbyte)
	}

	// write out ObjectMD
	status, errmsg = s.s3io.WriteObjectMD(bkname, objname, b)
	if status != StatusOK {
		glog.Errorln("failed to write ObjectMD", s.req.requuid, bkname, objname, status, errmsg)
		http.Error(w, errmsg, status)
		return
	}

	glog.V(0).Infoln("create object success", s.req.requuid, bkname, objname, s.md.Smd.Etag,
		"compress", len(mdbyte), len(b))

	w.Header().Set(ETag, s.md.Smd.Etag)
	w.WriteHeader(status)
}
