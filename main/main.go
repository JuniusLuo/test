package main

import (
	"flag"
	"net/http"
	"test"

	"github.com/golang/glog"
)

func main() {

	flag.Parse()

	s := test.NewS3Server()
	if s == nil {
		return
	}

	glog.Fatal(http.ListenAndServe(":8080", s))
}
