package main

import (
	"flag"
	"net/http"
	"test"

	"github.com/golang/glog"
)

// log level definitons:
//	0 - enabled by default, just in case wants to disable
//  1 - some more useful log
//  2 - reserve
//	3 - reserve
//	4 - trace
func main() {

	flag.Parse()

	s := test.NewS3Server()
	if s == nil {
		return
	}

	glog.Fatal(http.ListenAndServe(":8080", s))
}
