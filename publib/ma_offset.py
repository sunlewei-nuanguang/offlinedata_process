#encoding=utf-8

import datetime
import logging

import numpy
from collections import deque, defaultdict
from publib.initializer import initializer
from publib.utils import truncate_minute, datetime_range
from trade_row import DepthRow, minute_average, get_usdt_ma

MA_LENGTH = 0.0398300196037269
FUTURE_DAYS_DELAY = 1
COMMISSION_RATE = 0.001


class InterExchangeData(DepthRow):

    def __init__(self, json_data, data_type):
        self.trade_best = json_data["trade_best"]
        self.forex_rate = json_data["forex_rate"]
        self.time = datetime.datetime.fromtimestamp(json_data["time"])
        self.data_type = data_type


class InterExchangeDataRoute(DepthRow):

    def __init__(self, json_data, data_type, forex_rate_name="forex_rate_otc", is_divide=True):
        if is_divide:
            self.trade_best = 1/json_data["trade_best"]
            self.forex_rate = 1/json_data[forex_rate_name]
        else:
            self.trade_best = json_data["trade_best"]
            self.forex_rate = json_data[forex_rate_name]
        self.time = datetime.datetime.fromtimestamp(json_data["time"])
        self.data_type = data_type


def get_ma(time_line, days):
    '''
    获取时间线的 days 天移动平均
    '''
    timedelta = datetime.timedelta(days=days)
    buf = deque()
    buf_sum = 0
    ma_list = []
    for pair in time_line:
        buf.append(pair)
        time, value = pair
        buf_sum += value
        while buf[0][0] < time - timedelta:
            t, v = buf.popleft()
            buf_sum -= v
        ma_list.append((time, buf_sum / len(buf)))
    return ma_list


def get_spot_future(bars, min_time_diff):
    '''
    将合约初始化后得到的期货和现货数据组合起来
    '''
    last_spot = None
    last_future = None
    last_time = None
    for r in bars:
        if r.data_type in ("thisweek", "nextweek", "quarter"):
            last_future = r
        else:
            assert(r.data_type == "spot")
            last_spot = r
        if not last_spot or not last_future:
            continue
        if abs((last_spot.time - last_future.time).total_seconds()) > 3:
            continue

        if last_time is None or r.time - last_time > min_time_diff:
            yield r.time, last_spot, last_future
            last_time = r.time


def get_future_spot_index(bars, usdt_ma):
    '''
    按分钟计算平均期货现货价格比，绘图和计算移动平均使用
    '''
    result = minute_average((t, (last_future.ask + last_future.bid) / (last_spot.bid + last_spot.ask))
        for t, last_spot, last_future in get_spot_future(bars, datetime.timedelta(0)))
    if usdt_ma is not None:
        result = [(t, v / usdt_ma[t]) for t, v in result]
    return result


def get_route_index(all_data):
    '''
    按分钟计算平均币市汇率/银行间汇率，绘图和计算移动平均使用
    '''
    time_line = [(t, (lo.trade_best + lc.trade_best) / (lo.forex_rate + lc.forex_rate)) for t, lo, lc in all_data]
    minute_list = defaultdict(list)
    for t, v in time_line:
        minute_list[truncate_minute(t)].append(v)
    return sorted((t, numpy.mean(vl)) for t, vl in minute_list.items())

def limited_fill_gaps(time_begin, time_end, result, limit=datetime.timedelta(seconds=300)):
    # 如果这一分钟没有值，用上一分钟的值
    step = datetime.timedelta(seconds=60)
    first_minute = truncate_minute(time_begin)
    first_exist = min(result)
    if first_exist > first_minute:
        logging.warning("data missing from %s to %s", first_minute, first_exist)

    last_exist = first_exist
    for t in datetime_range(first_exist, time_end + step, step):
        if t in result:
            last_exist = t
        elif t - last_exist <= limit:
            result[t] = result[last_exist]


def test():
    for t, r in sorted(get_usdt_ma(datetime.datetime(2018, 9, 13), datetime.datetime(2018, 9, 15), datetime.timedelta(seconds=7200)).items()):
        logging.info("%s %.5f ", t, r)


if __name__ == "__main__":
    initializer.init()
    test()
