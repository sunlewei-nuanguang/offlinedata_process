#encoding=utf-8

import datetime
import logging


def truncate_minute(d):
    '''
    将时间按分钟截取
    '''
    if isinstance(d, datetime.datetime):
        return datetime.datetime(d.year, d.month, d.day, d.hour, d.minute)
    if isinstance(d, datetime.date):
        return datetime.datetime(d.year, d.month, d.day)
    logging.fatal(d)
    raise ValueError("parament should be datetime or date")


def minute_ceil(d):
    '''
    将时间按分钟截取
    '''
    result = truncate_minute(d)
    if result < d:
        result += datetime.timedelta(minutes=1)
    return result


def datetime_range(start, end, step=datetime.timedelta(days=1)):
    current = start
    if step.total_seconds() > 0:
        while current < end:
            yield current
            current += step
    else:
        while current > end:
            yield current
            current += step
