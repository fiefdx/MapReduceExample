# -*- coding: utf-8 -*-
'''
Created on 2016-12-22
'''

class TaskProcesser(object):
    name = ""

    @classmethod
    def prepare(cls, x):
        return (cls.name, x)

    def __init__(self):
        pass

    def map(self, x):
        return (self.name, x)

    def reduce(self, x, y):
        return x

class SumProcesser(TaskProcesser):
    name = "sum"

    def __init__(self):
        pass

    def map(self, x):
        return (self.name, x)

    def reduce(self, x, y):
        return x + y