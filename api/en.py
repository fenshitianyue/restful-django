#!/usr/bin/python2
# -*- coding:UTF-8 -*-

# d = dict({'key1' : 'value1', 'key2' : 'value2'})
#
# print d.keys()[0] + ':' + d[d.keys()[0]]
#
# for key, value in d.items():
#     print key + ':' + value

# raw_field = 'field5__count_distinct'
raw_field = 'field5__count'

field = raw_field[:raw_field.find('_')]

print field
