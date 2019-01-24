
#coding:utf-8
#author:shiyuming

import sys
import pymongo
import traceback
from pymongo import MongoClient
from pymongo import DESCENDING
import datetime,time
import logging
import re
import time
import json
import copy
import redis
import hashlib
import pickle
import os




class TimeCostMonitor:
    '''
    监控耗时
    '''
    def __init__(self):
        self.begin = time.time()

    def reset(self):
        self.begin = time.time()

    def monitor(self, label):
        cost = time.time() - self.begin
        logging.debug("耗时监控 %s cost [%s] 毫秒" % (label, cost * 1000))
        self.begin = time.time()







