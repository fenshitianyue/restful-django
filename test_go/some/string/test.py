#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ctypes

get_str = ctypes.CDLL('./libgetstr.so').get_string

get_str.argtypes = [ctypes.c_char_p]
get_str.restype = ctypes.c_char_p

if __name__ == '__main__':
    s = 'hello'
    print get_str(s)
