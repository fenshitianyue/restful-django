#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ctypes

lib = ctypes.cdll.LoadLibrary('./libconvert.so')
lib.convertsql.argtypes = [ctypes.c_char_p]
lib.convertsql.restype = ctypes.c_char_p

# convertsql = ctypes.CDLL('./libconvert.so').convertsql
# convertsql.argtypes = [ctypes.c_char_p]
# convertsql.restype = ctypes.c_char_p


if __name__ == '__main__':
    # sql = "select * from aaa where a = 1 and x = 'three man' and create_time between '2015-01-01T00:00:00+0800' and '2016-01-01T00:00:00+0800' and process_id > 1 order by id desc limit 100,10"
    # sql = "select * from some_table_name where field1 = 'test' and field2 > 5"
    # sql = "select test.field1. test.field2, sum(test.field4) as field4__sum, count(distinct test.field5) as field5__count_distinct, count(test.field3) as field3__count, avg(test.field4) as field4__avg from test where test.field1 = 'test1' and test.field3 like '%some%' and test.field2 > 5"
    # print sql
    sql = "select field1, field2, count(field3), avg(field4), sum(field4), count(distinct field5) as f5_cnt from table1 where field1='value1' and field2 > 5 and field3 like 'substr1' group by field1, field2 order by field1, field2 desc limit 2000"
    result = lib.convertsql(sql)
    print '------------------------------------------------------------------------------------------------'
    # print type(result)
    print result

