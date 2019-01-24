# coding: utf-8
# author:Kebin 

import os
import argparse
import logging

from .debug_log import setup_debug_log


LOG_LEVELS = {
    'NOTSET': logging.NOTSET, 'DEBUG': logging.DEBUG, 'INFO': logging.INFO,
    'WARNING': logging.WARNING, 'ERROR': logging.ERROR, 'CRITICAL': logging.CRITICAL,
    'FATAL': logging.FATAL}


class Initializer(object):
    '''脚本初始化工具，用来接收命令行参数，调用 init 来解析命令行参数并初始化日志。'''

    def __init__(self):
        self._parser = argparse.ArgumentParser()
        self._parser.add_argument(
            '--logtostderr', action='store_true', help='设置来打印日志到 stderr')
        self._parser.add_argument(
            '--disable_log_prefix', action='store_true', help='disable log prefix')
        self._parser.add_argument(
            '--loglevel', type=str, default='INFO',
            choices=list(LOG_LEVELS.keys()) + [str(i) for i in range(60)], help=(
                '日志打印等级，数值越高打印的日志越少。可以传标准的日志等级描述'
                '（字符串）或者整数。 (例如: INFO, ERROR, 15... INFO = 20, ERROR = 40)'))
        self._parser.add_argument(
            '--rotate_when', type=str, default='midnight',
            help='"S" for Seconds, "M" for Minutes, "H" for Hours, "D" for Days, "midnight" for Roll over at midnight')
        self._parser.add_argument(
            '--logdir', type=str, default=os.environ.get('PWD', '.'), help='存储日志的文件夹')
        self.args = argparse.Namespace()

    def add_argument(self, *args, **kwargs):
        '''可以像 argparse.ArgumentParser.add_argument 一样使用。

        调用 add_argument 以后就可以通过 self.args 取到默认值。
        '''
        self._parser.add_argument(*args, **kwargs)
        self.set_default(*args, **kwargs)

    def set_default(self, dest, **kwargs):
        '''把 ArgumentParser 里存储的默认值取到 Initializer'''
        dest = dest.strip('-')
        setattr(self.args, dest, self._parser.get_default(dest))

    def init(self):
        '''脚本启动时调用，会初始化所有通过 Initializer 设置的命令行参数和日志配置。'''
        self.args = self._parser.parse_args()
        if self.args.loglevel.isdigit():
            loglevel = int(self.args.loglevel)
        else:
            loglevel = LOG_LEVELS[self.args.loglevel]
            if self.args.disable_log_prefix:
                log_format = '%(message)s'
            else:
                log_format = '%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s'
        setup_debug_log(loglevel, self.args.logtostderr, self.args.logdir, log_format, self.args.rotate_when)


initializer = Initializer()
