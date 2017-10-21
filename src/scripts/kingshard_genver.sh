#!/bin/bash

version=`git log --date=short --pretty=format:"%cd @%h" -1`
if [ $? -ne 0 ]; then
    version="not a git repo"
fi

compile=`date +"%F %T"`

cat << EOF | gofmt > core/hack/version.go
package hack

const (
    Version = "$version"
    Compile = "$compile"
)
EOF