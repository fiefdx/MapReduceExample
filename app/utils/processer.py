# -*- coding: utf-8 -*-
'''
Created on 2016-12-23
''' 

import os
import json
import logging
from threading import Thread
from multiprocessing import Process, Queue

from config import CONFIG
import logger

LOG = logging.getLogger(__name__)

TaskQueue = Queue(5000)
ResultQueue = Queue(5000)

class Processer(Thread):
    def __init__(self, pid, task_queue, result_queue, mapping):
        Thread.__init__(self)
        self.pid = pid
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.mapping = mapping

    def run(self):
        LOG = logging.getLogger("worker")
        LOG.info("Processer(%03d) start", self.pid)
        try:
            while True:
                task = self.task_queue.get()
                if task != "mission_complete":
                    cmd, job = task
                    LOG.debug("processing task: %s", task)
                    processer = self.mapping.get(cmd)
                    if processer:
                        r = processer.map(job)
                        self.result_queue.put(r)
                    LOG.debug("processed task: %s", cmd)
                else:
                    break
            self.task_queue.put("mission_complete")
        except Exception, e:
            LOG.exception(e)
        LOG.info("Processer(%03d) exit", self.pid)

class Worker(Process):
    def __init__(self, wid, task_queue, result_queue, mapping):
        Process.__init__(self)
        self.wid = wid
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.mapping = mapping

    def run(self):
        logger.config_logging(logger_name = "worker",
                              file_name = ("worker_%s" % self.wid + ".log"),
                              log_level = CONFIG["log_level"],
                              dir_name = CONFIG["log_path"],
                              day_rotate = False,
                              when = "D",
                              interval = 1,
                              max_size = 20,
                              backup_count = 5,
                              console = True)
        LOG = logging.getLogger("worker")
        LOG.propagate = False
        LOG.info("Worker(%03d) start", self.wid)
        try:
            threads = []
            for i in xrange(CONFIG["threads"]):
                t = Processer(i, self.task_queue, self.result_queue, self.mapping)
                threads.append(t)

            for t in threads:
                t.start()

            for t in threads:
                t.join()
        except Exception, e:
            LOG.exception(e)
        LOG.info("Worker(%03d) exit", self.wid)

class Collector(Process):
    def __init__(self, data_queue, result_file_name, mapping):
        Process.__init__(self)
        self.data_queue = data_queue
        self.datas = {}
        self.result_file_name = result_file_name
        self.mapping = mapping

    def run(self):
        logger.config_logging(logger_name = "collector",
                              file_name = "collector.log",
                              log_level = CONFIG["log_level"],
                              dir_name = CONFIG["log_path"],
                              day_rotate = False,
                              when = "D",
                              interval = 1,
                              max_size = 20,
                              backup_count = 5,
                              console = True)
        LOG = logging.getLogger("collector")
        LOG.propagate = False
        LOG.info("Collector start")
        try:
            while True:
                data = self.data_queue.get()
                LOG.debug("get data: %s", data)
                if data != "mission_complete":
                    if self.datas.has_key(data[0]):
                        self.datas[data[0]] = self.mapping.get(data[0]).reduce(data[1], self.datas[data[0]])
                    else:
                        self.datas[data[0]] = data[1]
                else:
                    break
            result_file_path = os.path.join(CONFIG["data_path"], self.result_file_name)
            fp = open(result_file_path, "wb")
            fp.write(json.dumps(self.datas, indent = 4))
            fp.close()
        except Exception, e:
            LOG.exception(e)
        LOG.info("Collector exit")
