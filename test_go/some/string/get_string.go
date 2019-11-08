package main

import (
  "C"
  "reflect"
  "fmt"
)

//export get_string
func get_string(s *C.char) *C.char {
  ss := C.GoString(s)
  fmt.Println(reflect.TypeOf(ss))
  fmt.Println(ss)
  fmt.Println("--------------------")
  return C.CString(ss)
}

func main() {
}
