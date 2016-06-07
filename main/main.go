package main

import (
	"flag"
	"net/http"
	"test"

	"github.com/golang/glog"
)

// log level definitons:
//	0 - enabled by default, just in case wants to disable
//  1 - basic object operation log
//  2 - object data read/write detail log
//	3 - reserve
//	4 - reserve
//	5 - trace
func main() {

	flag.Parse()

	s := test.NewS3Server()
	if s == nil {
		return
	}

	glog.Fatal(http.ListenAndServe(":8080", s))
}
