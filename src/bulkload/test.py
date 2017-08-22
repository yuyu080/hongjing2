#!/usr/bin/python
# -*- coding: utf-8 -*-
import time

def g():
    print '1'
    x=yield 'hello'
    print '2','x=',x
    y=5+(yield  x)
    print '3','y=',y
#g().next()
# f = g()
# f.next()
# #f.next()

def func(n):
    for i in range(0, n):
        print('func: ', i)
        yield i

# f = func(10)
# while True:
#     print(next(f))
#     time.sleep(1)


def func(n):
    for i in range(0, n):
        arg = yield i
        time.sleep(1)
        print('func:', arg)

f = func(10)
while True:
    print('main:', next(f))
    #print('main2:', f.send(100))
    #time.sleep(1)