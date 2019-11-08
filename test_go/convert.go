package main

import (
  "C"
//  "fmt"
//  "reflect"
  "github.com/cch123/elasticsql"
)



//export convertsql
func convertsql(sql *C.char) *C.char {
  result, _, _ := elasticsql.Convert(C.GoString(sql))
  // fmt.Println(reflect.TypeOf(result))
  // fmt.Println(result)
  // fmt.Println("-----------------------")
  // fmt.Println(reflect.TypeOf(C.CString(result)))
  // fmt.Println(C.CString(result))
  return C.CString(result)
}

func main(){
}


