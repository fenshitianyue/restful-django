#!/usr/bin/python2
# -*- coding:UTF-8 -*-

def top_func(string):
    try:
        result = lower_func(string)
        print 'result: ', result
    except RuntimeError, err:
        print err.message
    except ValueError, err:
        print err.message
    except Exception as err:
        print "Else error: ", err.message

def lower_func(string):
    if string is None:
        raise RuntimeError('string not found')
    if string not in 'world':
        raise ValueError('string not in "world"')
    a = 1 / 0
    return a

if __name__ == '__main__':
    top_func('world')
