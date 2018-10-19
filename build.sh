#!/bin/bash
set -x #echo on

GOOS=linux go build -o flights main.go
