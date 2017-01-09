# -*- coding: utf-8 -*-
'''
Created on 2016-12-23
''' 

import os
import json
import signal
import logging
import threading
from threading import Thread
from multiprocessing import Process, Queue

from config import CONFIG
import logger

LOG = logging.getLogger(__name__)

TaskQueue = Queue(CONFIG["parallel"] * CONFIG["threads"] * 2)
ResultQueue = Queue(CONFIG["parallel"] * CONFIG["threads"] * 2)
StopSignal = "mission_complete"

class StoppableThread(Thread):
    """
    Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition.
    """

    def __init__(self):
        super(StoppableThread, self).__init__()
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

class Processer(StoppableThread):
    def __init__(self, pid, task_queue, result_queue, mapping):
        StoppableThread.__init__(self)
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
                if not self.stopped():
                    task = self.task_queue.get()
                    if task != StopSignal:
                        cmd, job = task
                        LOG.debug("processing task: %s", task)
                        processer = self.mapping.get(cmd)
                        if processer:
                            r = processer.map(job)
                            self.result_queue.put(r)
                        LOG.debug("processed task: %s", cmd)
                    else:
                        break
                else:
                    LOG.info("Processer(%03d) exit by signal!", self.pid)
                    break
            self.task_queue.put(StopSignal)
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

    def sig_handler(self, sig, frame):
        LOG.warning("Worker(%03d) Caught signal: %s", self.wid, sig)

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
            signal.signal(signal.SIGTERM, self.sig_handler)
            signal.signal(signal.SIGINT, self.sig_handler)
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

class Dispatcher(Process):
    def __init__(self, task_queue, mapping):
        Process.__init__(self)
        self.task_queue = task_queue
        self.mapping = mapping
        self.stop = False

    def sig_handler(self, sig, frame):
        LOG.warning("Dispatcher Caught signal: %s", sig)
        self.stop = True

    def run(self):
        logger.config_logging(logger_name = "dispatcher",
                              file_name = "dispatcher.log",
                              log_level = CONFIG["log_level"],
                              dir_name = CONFIG["log_path"],
                              day_rotate = False,
                              when = "D",
                              interval = 1,
                              max_size = 20,
                              backup_count = 5,
                              console = True)
        LOG = logging.getLogger("dispatcher")
        LOG.propagate = False
        LOG.info("Dispatcher start")
        try:
            signal.signal(signal.SIGTERM, self.sig_handler)
            signal.signal(signal.SIGINT, self.sig_handler)
            for name, task_processer in self.mapping.iter():
                LOG.info("dispatch: %s", name)
                if not self.stop:
                    for task in task_processer.iter():
                        if not self.stop:
                            self.task_queue.put(task)
                        else:
                            break
                else:
                    break
        except Exception, e:
            LOG.exception(e)
        self.task_queue.put(StopSignal)
        LOG.info("Dispatcher exit")

class Collector(Process):
    def __init__(self, data_queue, result_file_name, mapping):
        Process.__init__(self)
        self.data_queue = data_queue
        self.datas = {}
        self.result_file_name = result_file_name
        self.mapping = mapping

    def sig_handler(self, sig, frame):
        LOG.warning("Collector Caught signal: %s", sig)

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
            signal.signal(signal.SIGTERM, self.sig_handler)
            signal.signal(signal.SIGINT, self.sig_handler)
            while True:
                data = self.data_queue.get()
                LOG.debug("get data: %s", data)
                if data != StopSignal:
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
