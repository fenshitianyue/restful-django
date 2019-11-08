#!/bin/bash

rm lib*
go build -o libgetstr.so -buildmode=c-shared get_string.go
