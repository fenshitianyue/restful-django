package main

import "fmt"
import "github.com/cch123/elasticsql"

func main() {
  sql := "select * from some_table_name where filed1 = 'test' and field2 > 5"
  result, _, _ := elasticsql.Convert(sql)
  fmt.Println(result)
}
