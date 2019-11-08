#!/bin/bash

rm lib*
go build -o libconvert.so -buildmode=c-shared convert.go
