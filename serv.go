package test

import (
  "io"
  "net/http"

  "github.com/golang/glog"
)

type Server struct {

}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  glog.V(2).Infoln(r.Method, r.URL)

  switch r.Method {
  case "PUT":
  case "GET":
    s.hello(w, r)
  }
}

func (s *Server) hello(w http.ResponseWriter, r *http.Request) {
  io.WriteString(w, "Hello world!\n")
}
