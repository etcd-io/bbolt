#!/bin/bash

go list  --f '{{with $d:=.}}{{range .GoFiles}}{{$d.Dir}}/{{.}}{{"\n"}}{{end}}{{end}}' ./... | xargs go run golang.org/x/tools/cmd/goimports@latest -w -local go.etcd.io
