package main

import (
  "test"

  "net/http"

  "github.com/golang/glog"
)

func main() {
  s := new(test.Server)

  glog.Fatal(http.ListenAndServe(":8080", s))
}
