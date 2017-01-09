# -*- coding: utf-8 -*-
'''
Created on 2016-12-22
'''

import logging
import time

from config import CONFIG
from utils.processer import TaskQueue, ResultQueue, Worker, Collector
from model.task import SumProcesser
from model.mapping import Mapping
import logger

LOG = logging.getLogger(__name__)


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

    processes = []
    for i in xrange(CONFIG["parallel"]):
        p = Worker(i, TaskQueue, ResultQueue, mapping)
        processes.append(p)

    c = Collector(ResultQueue, "test", mapping)
    c.start()

    for p in processes:
        p.start()

    for x in xrange(10):
        TaskQueue.put(SumProcesser.prepare(x))
    TaskQueue.put("mission_complete")

    for p in processes:
        p.join()

    ResultQueue.put("mission_complete")
    c.join()

    end_time = time.time()
    LOG.info("End Script\nUse Time: %ss", end_time - start_time)
