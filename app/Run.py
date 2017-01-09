# -*- coding: utf-8 -*-
'''
Created on 2016-12-22
'''

import logging
import time
import signal

from config import CONFIG
from utils.processer import TaskQueue, ResultQueue, Worker, Collector, Dispatcher, StopSignal
from model.task import SumProcesser
from model.mapping import Mapping
import logger

LOG = logging.getLogger(__name__)

def sig_handler(sig, frame):
    LOG.warning("Caught signal: %s", sig)
    TaskQueue.put(StopSignal)

if __name__ == "__main__":
    logger.config_logging(
        file_name = "Run.log", 
        log_level = CONFIG["log_level"], 
        dir_name = CONFIG["log_path"], 
        day_rotate = False, 
        when = "D", 
        interval = 1, 
        max_size = 50,
        backup_count = 5, 
        console = CONFIG["console"]
    )
    LOG.info("Start Script")
    start_time = time.time()

    mapping = Mapping()
    mapping.add(SumProcesser())

    workers = []
    for i in xrange(CONFIG["parallel"]):
        w = Worker(i, TaskQueue, ResultQueue, mapping)
        workers.append(w)

    c = Collector(ResultQueue, CONFIG["result_file_name"], mapping)
    c.start()

    d = Dispatcher(TaskQueue, mapping)
    d.start()

    for w in workers:
        w.start()

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    for w in workers:
        w.join()

    ResultQueue.put(StopSignal)
    c.join()
    d.join()

    end_time = time.time()
    LOG.info("End Script\nUse Time: %ss", end_time - start_time)
