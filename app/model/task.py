# -*- coding: utf-8 -*-
'''
Created on 2016-12-22
'''

class TaskProcesser(object):
    name = ""

    def __init__(self):
        pass

    def iter(self):
        yield (self.name, None)

    def map(self, x):
        return (self.name, x)

    def reduce(self, x, y):
        return x

class SumProcesser(TaskProcesser):
    name = "sum"

    def __init__(self):
        pass

    def iter(self):
        for x in xrange(10000000000):
            yield (self.name, x)

    def map(self, x):
        return (self.name, x)

    def reduce(self, x, y):
        return x + y