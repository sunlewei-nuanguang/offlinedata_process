# coding: utf-8
# author:Kebin 

import time
import sys
import os
import logging
import logging.handlers
import __main__


def setup_debug_log(loglevel, logtostderr, logdir, format, rotate_when):
    '''日志初始化函数，只在 debug 环境下使用，默认在当前目录下打印所有日志。'''
    logging.root.setLevel(loglevel)
    log_format = logging.Formatter(format)

    try:
        fullname = __main__.__file__
    except AttributeError:
        fullname = sys.argv[0]
    script_name = os.path.basename(fullname)

    if not os.path.exists(logdir):
        os.makedirs(logdir)
    log_file_name = time.strftime('{}.%y%m%d_%H%M%S.log'.format(script_name))
    file_handler = logging.handlers.TimedRotatingFileHandler(os.path.join(logdir, log_file_name), rotate_when)
    file_handler.setFormatter(log_format)
    logging.root.addHandler(file_handler)

    # 打印日志到 stderr
    if logtostderr:
        console = logging.StreamHandler()
        console.setFormatter(log_format)
        logging.root.addHandler(console)

    # 建软连接到最新日志
    link_name = '%s/%s.log' % (logdir, script_name)
    if os.path.lexists(link_name):
        os.remove(link_name)
    os.symlink(log_file_name, link_name)
