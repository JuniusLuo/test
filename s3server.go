package test

import (
	"flag"
	"net/http"
	"strings"

	"github.com/golang/glog"
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
		glog.Infoln("put buckeet succeeded", bkname, r.URL, r.Host)
	} else {
		s.putObject(r, resp, bkname, objname)
	}
}

func (s *S3Server) putObject(r *http.Request, resp *http.Response, bkname string, objname string) {

}

func (s *S3Server) getOp(r *http.Request, resp *http.Response, bkname string, objname string) {
}
