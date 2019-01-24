# coding: utf-8

import logging
from knife.initializer import initializer
from knife.logging import syslogger
from knife.initializer.sample.test_arg import foo


def main():
    # 线下脚本应先调用初始化函数初始化日志配置，解析命令行参数。
    initializer.init()

    foo()
   
    # 建立 syslogger，DEBUG 模式下打到 logdir 里，线上打到对应的 syslog。
    child = syslogger('child')
    child.debug('debug from logger child')
    child.fatal('fatal from logger child')

    grandchild = syslogger('child.child')
    grandchild.warn('warn from logger child.child')
    grandchild.info('info from logger child.child')
    grandchild.debug('debug from logger child.child')

    # 也可以把 log 直接打到 root 节点上，但是线上不收集 root 日志。
    logging.debug('debug from root')
    logging.info('info from root')
    logging.fatal('fatal from root')

if __name__ == '__main__':
    main()
