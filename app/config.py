# -*- coding: utf-8 -*-
'''
Created on 2016-12-22
'''
try:
    import yaml
except ImportError:
    raise ImportError("Config module requires pyYAML package, please check if pyYAML is installed!")

from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import os
#
# default config
CONFIG = {}
try:
    # script in the app dir
    cwd = os.path.split(os.path.realpath(__file__))[0]
    configpath = os.path.join(cwd, "configuration.yml")
    localConf = load(stream = file(configpath), Loader = Loader)
    CONFIG.update(localConf)
    CONFIG["app_path"] = cwd
    CONFIG["config_path"] = configpath
    if not CONFIG.has_key("log_path"):
        CONFIG["log_path"] = "logs"
    if not CONFIG.has_key("data_path"):
        CONFIG["data_path"] = os.path.join(cwd, "DATA")
    if not os.path.exists(CONFIG["data_path"]):
        os.makedirs(CONFIG["data_path"])

except Exception, e:
    print e

if __name__ == "__main__":
    print "cwd: %s"%cwd
    print "configpath: %s"%configpath
    print "CONFIG: %s"%CONFIG



