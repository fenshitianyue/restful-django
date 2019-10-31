#!/usr/bin/python2
# -*- coding:UTF-8 -*-

def top_func(string):
    try:
        has_filter, has_agg = lower_func(string)
        print 'has_filter:', has_filter, 'has_agg:', has_agg
    except RuntimeError, err:
        print err.message
    except ValueError, err:
        print err.message

def lower_func(string):
    if string is None:
        raise RuntimeError('string not found')
    if string not in 'world':
        raise ValueError('string not in "world"')
    has_filter, has_agg = False, False
    return has_filter, has_agg

if __name__ == '__main__':
    top_func('world')
