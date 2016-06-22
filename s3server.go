package test

import (
	"flag"
	"io"
	"net/http"
	"strconv"
	"strings"
	"test/util"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"golang.org/x/net/context"
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

func (s *S3Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bkname, objname := s.getBucketAndObjectName(r)

	glog.V(5).Infoln(r.Method, r.URL, r.Host, "headers", r.Header)

	w.Header().Set(Server, ServerName)

	if bkname == "" {
		glog.Errorln("InvalidRequest, no bucketname", r.Method, r.URL, r.Host)
		http.Error(w, "InvalidRequest, no bucketname", InvalidRequest)
		return
	}

	// generate uuid as request id
	requuid, err := util.GenRequestID()
	if err != nil {
		glog.Errorln("failed to generate uuid for", r.Method, bkname, objname)
		http.Error(w, "failed to generate uuid", InternalError)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx = util.NewRequestContext(ctx, requuid)
	defer cancel()

	w.Header().Set(RequestID, requuid)

	glog.V(2).Infoln(requuid, r.Method, r.URL, r.Host, bkname, objname)

	switch r.Method {
	case "POST":
		http.Error(w, NotImplementedStr, NotImplemented)
	case "PUT":
		s.putOp(ctx, w, r, bkname, objname)
	case "GET":
		s.getOp(ctx, w, r, bkname, objname)
	case "HEAD":
		s.headOp(ctx, w, r, bkname, objname)
	case "DELETE":
		s.delOp(ctx, w, r, bkname, objname)
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

func (s *S3Server) putOp(ctx context.Context, w http.ResponseWriter, r *http.Request, bkname string, objname string) {
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
		p := NewS3PutObject(ctx, r, s.s3io, bkname, objname)
		p.PutObject(w, bkname, objname)
	}
}

func (s *S3Server) getOp(ctx context.Context, w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" || objname == BucketListOp {
			body, status, errmsg := s.s3io.GetBucket(bkname)
			if status != StatusOK {
				glog.Errorln("get bucket failed", util.GetReqIDFromContext(ctx), bkname, objname, status, errmsg)
				http.Error(w, errmsg, status)
				return
			}

			// GetBucket success, copy result to w
			n, err := io.Copy(w, body)
			if err != nil || n == 0 {
				glog.Errorln("get bucket failed", util.GetReqIDFromContext(ctx), bkname, objname, n, err)
				if n == 0 {
					// n == 0 is not valid, the list would at least have some xml string
					// read and write 0 data, w.Write may not be called in io.Copy
					w.WriteHeader(InternalError)
				}
			} else {
				glog.V(1).Infoln("get bucket success", util.GetReqIDFromContext(ctx), bkname, objname, n)
			}
		} else {
			glog.Errorln("not support get bucket operation", util.GetReqIDFromContext(ctx), bkname, objname)
			http.Error(w, NotImplementedStr, NotImplemented)
		}
	} else {
		s.getObjectOp(ctx, w, r, bkname, objname)
	}
}

func (s *S3Server) getObjectMD(ctx context.Context, r *http.Request, bkname string,
	objname string) (objmd *ObjectMD, status int, errmsg string) {
	// object get, read metadata object first
	b, status, errmsg := s.s3io.ReadObjectMD(bkname, objname)
	if status != StatusOK {
		glog.Errorln("failed to ReadObjectMD", util.GetReqIDFromContext(ctx), bkname, objname, status, errmsg)
		return nil, status, errmsg
	}

	// uncompress ObjectMD bytes
	mdbyte := b
	var err error
	if *cmp {
		mdbyte, err = snappy.Decode(nil, b)
		if err != nil {
			glog.Errorln("failed to uncompress ObjectMD", util.GetReqIDFromContext(ctx), bkname, objname, err)
			return nil, InternalError, "failed to uncompress ObjectMD"
		}

		glog.V(5).Infoln(util.GetReqIDFromContext(ctx), "compressed size", len(b), "original size", len(mdbyte), bkname, objname)
	}

	objmd = &ObjectMD{}
	err = proto.Unmarshal(mdbyte, objmd)
	if err != nil {
		glog.Errorln("failed to Unmarshal ObjectMD", util.GetReqIDFromContext(ctx), bkname, objname, err)
		return nil, InternalError, InternalErrorStr
	}

	glog.V(2).Infoln("successfully read object md", util.GetReqIDFromContext(ctx), bkname, objname,
		objmd.Smd, "totalParts", len(objmd.Data.DataParts))
	return objmd, StatusOK, StatusOKStr
}

func (s *S3Server) getObjectOp(ctx context.Context, w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	// object get, read metadata object
	objmd, status, errmsg := s.getObjectMD(ctx, r, bkname, objname)
	if status != StatusOK {
		glog.Errorln("getObjecct failed to get ObjectMD",
			util.GetReqIDFromContext(ctx), bkname, objname, status, errmsg)
		http.Error(w, errmsg, status)
		return
	}

	w.Header().Set(LastModified, time.Unix(objmd.Smd.Mtime, 0).UTC().Format(time.RFC1123))
	w.Header().Set(ETag, objmd.Smd.Etag)
	w.Header().Set(ContentLength, strconv.FormatInt(objmd.Smd.Size, 10))

	if objmd.Smd.Size == 0 {
		glog.V(1).Infoln("get object success, size 0", util.GetReqIDFromContext(ctx), bkname, objname)
		w.WriteHeader(status)
		return
	}

	// construct Body reader to read the corresponding data blocks
	rd := NewS3GetObject(ctx, r, s.s3io, objmd, bkname, objname)
	status, errmsg = rd.GetObject()
	if status != StatusOK {
		http.Error(w, errmsg, status)
		return
	}

	n, err := io.Copy(w, rd)
	if err != nil || n == 0 {
		// n == 0 is also an error,
		glog.Errorln("get object failed", util.GetReqIDFromContext(ctx), bkname, objname, n, err)
		if n == 0 {
			// n == 0 is also an error. if object size is not 0, will not reach here.
			// read and write 0 data, w.Write may not be called in io.Copy
			w.WriteHeader(InternalError)
		}
	} else {
		glog.V(1).Infoln("get object success", util.GetReqIDFromContext(ctx), bkname, objname, n, objmd.Smd)
	}

	// read done, close reader
	rd.closeChan()
}

func (s *S3Server) delOp(ctx context.Context, w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" {
			status, errmsg := s.s3io.DeleteBucket(bkname)
			if status != StatusOK {
				glog.Errorln("delete bucket failed", util.GetReqIDFromContext(ctx), bkname, status, errmsg)
				http.Error(w, errmsg, status)
				return
			}
			glog.Infoln("del bucket success", util.GetReqIDFromContext(ctx), bkname)
			w.WriteHeader(status)
		} else {
			glog.Errorln("NotImplemented delete bucket operation", util.GetReqIDFromContext(ctx), bkname, objname)
			http.Error(w, NotImplementedStr, NotImplemented)
		}
	} else {
		s.delObject(ctx, w, r, bkname, objname)
	}
}

func (s *S3Server) delObject(ctx context.Context, w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	// read object md

	// log it to the local fs (protected by EBS or the underline storage of VMWare)

	// return success, the background scanner will pick up from log
}

func (s *S3Server) headOp(ctx context.Context, w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	if s.isBucketOp(objname) {
		if objname == "" || objname == "/" {
			status, errmsg := s.s3io.HeadBucket(bkname)
			if status != StatusOK {
				glog.Errorln("failed to head bucket", util.GetReqIDFromContext(ctx), bkname, status, errmsg)
				http.Error(w, errmsg, status)
				return
			}

			glog.V(2).Infoln("head bucket success", util.GetReqIDFromContext(ctx), bkname)
			w.WriteHeader(status)
		} else {
			glog.Errorln("Invalid head bucket operation", util.GetReqIDFromContext(ctx), bkname, objname)
			http.Error(w, "Invalid head bucket operation", InvalidRequest)
		}
	} else {
		s.headObject(ctx, w, r, bkname, objname)
	}
}

func (s *S3Server) headObject(ctx context.Context, w http.ResponseWriter, r *http.Request, bkname string, objname string) {
	// get ObjectMD
	objmd, status, errmsg := s.getObjectMD(ctx, r, bkname, objname)
	if status != StatusOK {
		glog.Errorln("headObjecct failed to get ObjectMD",
			util.GetReqIDFromContext(ctx), bkname, objname, status, errmsg)
		http.Error(w, errmsg, status)
		return
	}

	glog.V(2).Infoln("head object success", util.GetReqIDFromContext(ctx), objmd.Smd)

	w.Header().Set(LastModified, time.Unix(objmd.Smd.Mtime, 0).UTC().Format(time.RFC1123))
	w.Header().Set(ETag, objmd.Smd.Etag)
	w.WriteHeader(StatusOK)
}
