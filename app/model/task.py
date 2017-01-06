# -*- coding: utf-8 -*-
'''
Created on 2016-12-22
'''

class Task(object):
    def __init__(self, x):
        self.name = ""
        self.x = x

    def prepare(self):
        return (self.name, self.x)

class TaskProcesser(object):
    def __init__(self):
        self.name = ""

    def map(self, x):
        return (self.name, x)

    def reduce(self, x, y):
        return x

class SumTask(Task):
    def __init__(self, x):
        self.name = "sum"
        self.x = x

    def prepare(self):
        return (self.name, self.x)

class SumProcesser(TaskProcesser):
    def __init__(self):
        self.name = "sum"

    def map(self, x):
        return (self.name, x)

    def reduce(self, x, y):
        return x + y