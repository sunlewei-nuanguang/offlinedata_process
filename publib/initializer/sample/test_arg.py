# coding: utf-8

import logging

from knife.initializer import initializer

# 这样设置参数可以在最外层脚本通过命令行参数修改，线上服务使用默认参数.
initializer.add_argument('--hello', default=15, type=int)
initializer.add_argument('--test_bool', action='store_true')


def foo():
    logging.warn('warn from foo')
    logging.info('hello = %s', initializer.args.hello)
    logging.info('test_bool = %s', initializer.args.test_bool)
