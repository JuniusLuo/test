package test

import (
	"errors"
	"io"
	"net/http"
	"test/util"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

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
	partNum  int
	part     *DataPart
	status   int
	errmsg   string
}

// S3GetObject is the class to handle object get and head
type S3GetObject struct {
	ctx     context.Context
	w       http.ResponseWriter
	r       *http.Request
	s3io    CloudIO
	bkname  string
	objname string
	requuid string

	objmd *ObjectMD

	// the read offset
	off int64
	// data part of the currBlock
	currPart dataPartReadResult
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

// NewS3GetObject creates a S3GetObject instance
func NewS3GetObject(ctx context.Context, r *http.Request, s3io CloudIO,
	md *ObjectMD, bkname string, objname string) *S3GetObject {
	s := new(S3GetObject)
	s.ctx = ctx
	s.requuid = s.requuid
	s.r = r
	s.s3io = s3io
	s.objmd = md
	s.bkname = bkname
	s.objname = objname
	return s
}

func (d *S3GetObject) isLastBlock(partNum int, blkIdx int) bool {
	data := d.objmd.Data
	totalParts := len(data.DataParts)

	lastPart := data.DataParts[totalParts-1]
	if partNum == totalParts-1 {
		l := len(lastPart.Blocks)
		if l == 0 {
			// last part may not have any block
			glog.V(5).Infoln("last part has no data, totalParts", totalParts, d.objmd.Smd)
			return true
		}
		glog.V(5).Infoln("reach the last block, totalParts", totalParts, "block", blkIdx, d.objmd.Smd)
		return blkIdx == l-1
	}

	return false
}

// check to make sure the read block is in currPart
func (d *S3GetObject) isValidReadBlock(partNum int, blkIdx int) bool {
	smd := d.objmd.Smd
	// sanity check
	if partNum != d.currPart.partNum {
		glog.Errorln("SanityError - ", d.requuid, "read block in part", partNum,
			"not in currPart", d.currPart.partName, smd)
		return false
	}

	// sanity check
	blkCount := len(d.currPart.part.Blocks)
	if blkIdx >= blkCount {
		glog.Errorln("SanityError - ", d.requuid, "read unexist block",
			blkIdx, "in part", d.currPart.partName, blkCount, smd.Bucket, smd.Name)
		return false
	}

	return true
}

func (d *S3GetObject) readBlock(partNum int, blkIdx int, b []byte) dataBlockReadResult {
	res := dataBlockReadResult{partNum: partNum, blkIdx: blkIdx, buf: b}

	// sanity check
	if !d.isValidReadBlock(partNum, blkIdx) {
		res := dataBlockReadResult{status: InternalError, errmsg: "read unexist block"}
		return res
	}

	smd := d.objmd.Smd
	dataPart := d.currPart.part
	res.blkmd5 = dataPart.Blocks[blkIdx]

	res.n, res.status, res.errmsg = d.s3io.ReadDataBlockRange(res.blkmd5, 0, res.buf)

	glog.V(2).Infoln("read block done", d.requuid, "part", partNum, "block", blkIdx,
		res.blkmd5, res.n, res.status, res.errmsg, smd.Bucket, smd.Name)

	if res.status == StatusOK && res.n != int(d.objmd.Data.BlockSize) &&
		!d.isLastBlock(partNum, blkIdx) {
		// read less data, could only happen for the last block
		glog.Errorln("not read full block", d.requuid, res.n,
			d.objmd.Data.BlockSize, dataPart.Name, blkIdx, smd.Bucket, smd.Name)
		res.status = InternalError
		res.errmsg = "read less data for a full block"
	}

	return res
}

func (d *S3GetObject) prefetchBlock(partNum int, blk int, b []byte) {
	glog.V(5).Infoln("prefetchBlock start", d.requuid,
		"part", partNum, "block", blk, d.objmd.Smd)

	if RandomFI() && !FIRandomSleep() {
		// simulate the connection broken and closeChan() is called
		glog.Errorln("FI at prefetchBlock, close d.closed chan", d.requuid, blk, d.objmd.Smd)
		d.closeChan()
	}

	res := d.readBlock(partNum, blk, b)

	select {
	case d.blockChan <- res:
		glog.V(5).Infoln("prefetchBlock sent to chan done", d.requuid, blk, d.objmd.Smd)
	case <-d.closed:
		glog.Errorln("stop prefetchBlock, reader closed", d.requuid, partNum, blk, d.objmd.Smd)
	case <-time.After(RWTimeOutSecs * time.Second):
		glog.Errorln("stop prefetchBlock, timeout", d.requuid, partNum, blk, d.objmd.Smd)
	}
}

func (d *S3GetObject) prefetchPart(partNum int) {
	glog.V(5).Infoln("prefetchPart start", d.requuid, partNum, d.objmd.Smd)

	partName := util.GenPartName(d.objmd.Uuid, partNum)

	res := dataPartReadResult{partName: partName, partNum: partNum,
		status: StatusOK, errmsg: StatusOKStr}

	b, status, errmsg := d.s3io.ReadDataPart(d.objmd.Smd.Bucket, partName)
	if status == StatusOK {
		part := &DataPart{}
		err := proto.Unmarshal(b, part)
		if err != nil {
			glog.Errorln("failed to Unmarshal DataPart", d.requuid, partNum, err, d.objmd.Smd)
			res.status = InternalError
			res.errmsg = "failed to Unmarshal DataPart"
		} else {
			glog.V(5).Infoln("prefetchPart success", d.requuid, partNum, len(part.Blocks), d.objmd.Smd)
			res.part = part
		}
	} else {
		glog.Errorln("failed to ReadDataPart", d.requuid, partNum, status, errmsg, d.objmd.Smd)
		res.status = status
		res.errmsg = errmsg
	}

	select {
	case d.partChan <- res:
		glog.V(5).Infoln("prefetchPart sent to chan done", d.requuid, partNum, d.objmd.Smd)
	case <-d.closed:
		glog.Errorln("stop prefetchPart, reader closed", d.requuid, partNum, d.objmd.Smd)
	case <-time.After(RWTimeOutSecs * time.Second):
		glog.Errorln("stop prefetchPart, timeout", d.requuid, partNum, d.objmd.Smd)
	}
}

func (d *S3GetObject) closeChan() {
	glog.V(5).Infoln("closeChan", d.off, d.objmd.Smd.Size)

	// close the "closed" channel, so both prefetchBlock() and Read() can exit
	close(d.closed)
}

func (d *S3GetObject) waitPrefetchBlock(partNum int, blkInPart int) error {
	select {
	case nextBlock := <-d.blockChan:
		d.waitBlock = false

		if nextBlock.status != StatusOK {
			glog.Errorln("failed to prefetch block", d.requuid,
				"part", nextBlock.partNum, "block", nextBlock.blkIdx, nextBlock.status, nextBlock.errmsg)
			return errors.New(nextBlock.errmsg)
		}

		// sanity check
		if nextBlock.partNum != partNum || nextBlock.blkIdx != blkInPart {
			glog.Errorln("the prefetch block is not the next read block", d.requuid,
				"part", nextBlock.partNum, "block", nextBlock.blkIdx,
				"target part", partNum, "block", blkInPart, d.objmd.Smd)
			return errors.New(InternalErrorStr)
		}

		glog.V(5).Infoln("get the prefetch block", d.requuid,
			"part", partNum, "block", blkInPart, "read offset", d.off, d.objmd.Smd)

		// the next block is back, switch the current block to the next block
		oldbuf := d.currBlock.buf
		d.currBlock = nextBlock

		// prefetch the next block if necessary
		if d.currBlock.status == StatusOK && !d.isLastBlock(partNum, blkInPart) {
			if blkInPart == int(d.objmd.Data.MaxBlocks-1) {
				// read the last block in the currPart, wait prefetch part
				err := d.waitPrefetchPart()
				if err != nil {
					return err
				}

				// prefetch the first block in the next part
				if len(d.currPart.part.Blocks) != 0 {
					d.waitBlock = true
					go d.prefetchBlock(d.currPart.partNum, 0, oldbuf)
				}
			} else {
				// prefetch the next block in the same part
				d.waitBlock = true
				go d.prefetchBlock(partNum, blkInPart+1, oldbuf)
			}
		}
		return nil
	case <-time.After(RWTimeOutSecs * time.Second):
		glog.Errorln("waitPrefetchBlock timeout", d.requuid, "part", partNum,
			"block", blkInPart, "read offset", d.off, d.objmd.Smd)
		return errors.New("waitPrefetchBlock timeout")
	}
}

func (d *S3GetObject) waitPrefetchPart() error {
	totalParts := len(d.objmd.Data.DataParts)

	if !d.waitPart {
		// no more part to prefetch, the current part must be the last-1 part.
		if d.currPart.partNum != totalParts-2 {
			glog.Errorln("SanityError - ", d.requuid, "currPart", d.currPart.partNum,
				"is not the last-2 part, totalParts", totalParts, d.objmd.Smd)
			return errors.New(InternalErrorStr)
		}

		// set the currPart to the last part
		glog.V(2).Infoln("set the last part as currPart", d.requuid,
			"currPart", d.currPart.partNum, "totalParts", totalParts, d.objmd.Smd)

		part := d.objmd.Data.DataParts[totalParts-1]
		res := dataPartReadResult{partName: part.Name, partNum: totalParts - 1, part: part,
			status: StatusOK, errmsg: StatusOKStr}
		d.currPart = res
		return nil
	}

	glog.V(5).Infoln("wait the prefetch part", d.requuid, "currPart", d.currPart.partNum, d.objmd.Smd)

	select {
	case nextPart := <-d.partChan:
		d.waitPart = false

		if nextPart.status != StatusOK {
			glog.Errorln("failed to prefetch part", d.requuid,
				"part", nextPart.partNum, nextPart.status, nextPart.errmsg)
			return errors.New(nextPart.errmsg)
		}

		glog.V(5).Infoln("get the prefetch part", d.requuid,
			"part", nextPart.partNum, "totalParts", totalParts, d.objmd.Smd)

		// the next block is back, switch the current block to the next block
		d.currPart = nextPart

		// if not last-1 part, prefetch the next part
		if d.currPart.partNum < totalParts-2 {
			d.waitPart = true
			go d.prefetchPart(d.currPart.partNum + 1)
		}
		return nil
	case <-d.closed:
		glog.Errorln("waitPrefetchPart reader closed", d.requuid, "currPart", d.currPart.partNum, d.objmd.Smd)
		return errors.New("connection closed")
	case <-time.After(RWTimeOutSecs * time.Second):
		glog.Errorln("waitPrefetchPart timeout", d.requuid, "currPart", d.currPart.partNum, d.objmd.Smd)
		return errors.New("read timeout")
	}
}

func (d *S3GetObject) Read(p []byte) (n int, err error) {
	if d.off >= d.objmd.Smd.Size {
		glog.V(1).Infoln("finish read object data", d.requuid, d.objmd.Smd)
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
			glog.Errorln("no prefetch task", d.requuid, "part", partNum,
				"block", blkIdx, "read offset", d.off, d.objmd.Smd)
			return 0, errors.New("InternalError, no prefetch task")
		}

		glog.V(5).Infoln("wait the prefetch block", d.requuid, "part", partNum,
			"block", blkIdx, "read offset", d.off, d.objmd.Smd)

		if RandomFI() && !FIRandomSleep() {
			// simulate the connection broken and Close() is called
			// Q: looks the ongoing Read still goes through, d.closed looks not used here.
			glog.Errorln("FI at Read, close d.closed chan",
				d.requuid, blkIdx, d.off, d.objmd.Smd)
			d.closeChan()
		}

		err := d.waitPrefetchBlock(partNum, blkIdx)
		if err != nil {
			return 0, err
		}
	}

	// check the current block read status
	if d.currBlock.status != StatusOK {
		glog.Errorln("read data block failed", d.requuid, "part", partNum, "block", blkIdx,
			"read offset", d.off, d.currBlock.status, d.currBlock.errmsg, d.objmd.Smd)
		return 0, errors.New(d.currBlock.errmsg)
	}

	// fill data from the current block
	glog.V(2).Infoln("fill data from currBlock", d.requuid, "part", partNum,
		"block", blkIdx, blockOff, "block len", d.currBlock.n, "read offset", d.off, d.objmd.Smd)

	endOff := blockOff + len(p)
	if endOff <= d.currBlock.n {
		// currBlock has more data than p
		glog.V(5).Infoln("currBlock has enough data", d.requuid,
			"block", blkIdx, blockOff, "end", endOff)

		copy(p, d.currBlock.buf[blockOff:endOff])
		n = len(p)
	} else {
		// p could have more data than the rest in currBlock
		// TODO copy the rest data from the next block
		glog.V(5).Infoln("read the end of currBlock", d.requuid,
			"block", blkIdx, blockOff, "end", endOff)

		copy(p, d.currBlock.buf[blockOff:d.currBlock.n])
		n = d.currBlock.n - blockOff
	}

	d.off += int64(n)

	if d.off == d.objmd.Smd.Size {
		return n, io.EOF
	}

	return n, nil
}

func (d *S3GetObject) GetObject() (status int, errmsg string) {
	d.closed = make(chan bool)

	// synchronously read the first block
	b := make([]byte, d.objmd.Data.BlockSize)
	d.currPart = dataPartReadResult{partName: util.GenPartName(d.objmd.Uuid, 0), partNum: 0,
		part: d.objmd.Data.DataParts[0], status: StatusOK, errmsg: StatusOKStr}
	d.currBlock = d.readBlock(0, 0, b)

	// check the first block read status
	if d.currBlock.status != StatusOK {
		glog.Errorln("read first data block failed",
			d.requuid, d.objmd.Data.DataParts[0].Blocks[0],
			d.currBlock.status, d.currBlock.errmsg, d.bkname, d.objname)
		return d.currBlock.status, d.currBlock.errmsg
	}

	// if there are more data to read, start the prefetch task
	if d.objmd.Smd.Size > int64(d.objmd.Data.BlockSize) {
		d.blockChan = make(chan dataBlockReadResult)
		nextbuf := make([]byte, d.objmd.Data.BlockSize)
		d.waitBlock = true
		go d.prefetchBlock(0, 1, nextbuf)
	}

	// if there are more than 2 parts, start the prefetch task
	if len(d.objmd.Data.DataParts) > 2 {
		d.partChan = make(chan dataPartReadResult)
		d.waitPart = true
		go d.prefetchPart(1)
	}

	return StatusOK, StatusOKStr
}
