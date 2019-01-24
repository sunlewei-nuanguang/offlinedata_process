#encoding=utf-8

import time
import numpy
import datetime
import logging

from collections import defaultdict
from pymongo import MongoClient
from mongo_tools import last_updated
from mongoengine import IntField, FloatField, DateTimeField, Document
from initializer import initializer
from utils import minute_ceil, truncate_minute, datetime_range


ALL_OKEXFT_TOKENS = ["btc", "eos", "eth", "xrp", "ltc"]
TWS_CURRENCY = {"JPY", "KRW"}

OKEXFT_BLACKLIST = [(datetime.datetime(2018, 12, 26, 8), datetime.datetime(2018, 12, 26, 8))]

trade_mongo_client = MongoClient(host="172.31.140.86", port=28888, username="adminall", password="moveall")
miner_mongo_client = MongoClient(host="172.31.140.86", port=27888, username="adminall", password="moveall")
usdt_mongo_client = MongoClient(host="172.31.140.84", port=27777, username="adminall", password="moveall")
finance_mongo_client = MongoClient(host="172.31.140.205", port=27777, username="adminall", password="moveall")
server1_mongo_client = MongoClient(host="172.31.140.84", port=27777, username="adminall", password="moveall")

initializer.add_argument("--usdt_half_life_seconds", default=7200, type=float)


def get_data_internal(mongo_client, database, table_name, condition):
    return list(mongo_client[database][table_name].find(condition))


def get_data(mongo_client, database, table_name, time_multiplier, time_begin, time_end, add_conditions=None):
    '''
    从 mongodb 获取数据
    '''
    condition = {"time": {"$gte": int(time_multiplier * time.mktime(time_begin.timetuple())), "$lt": int(time_multiplier * time.mktime(time_end.timetuple()))}}

    if add_conditions is not None:
        condition.update(add_conditions)

    logging.info("%s@%s where %s" % (table_name, database, condition))
    ret = get_data_internal(mongo_client, database, table_name, condition)
    logging.info('%s@%s get mongo data complete' % (table_name, database))
    return ret


def get_interexchange(mongo_client, exchange1, exchange2, time_begin, time_end, database="miner_fb_best_close_60s", forex_rate_name="forex_rate_otc"):
    return get_data(mongo_client, database, "%s_%s_trade_best" % (exchange1, exchange2), 1, time_begin, time_end, add_conditions={forex_rate_name: {'$ne': -1}})


def get_spot(token, time_begin, time_end, exchange="okex", quote="usdt"):
    '''
    从 mongodb 获取现货深度数据
    '''
    return get_data(trade_mongo_client, exchange, "%s_%s_trade" % (token, quote), 1000, time_begin, time_end)


def get_future(token, future_type, time_begin, time_end, exchange="okexft"):
    '''
    从 mongodb 获取期货深度数据
    '''
    return get_data(trade_mongo_client, exchange, "%s%s_usd_trade"%(token, future_type), 1000, time_begin, time_end)


def get_forex(currency, time_begin, time_end):
    table_name = 'huilv_item_new'
    if(currency == 'JPY'):
        table_name = 'huilv_item_new_tws_fx'
    return [(datetime.datetime.fromtimestamp(row["time"] / 1000), 1.0 / row["refePrice"])
        for row in get_data(finance_mongo_client, "finance_info", table_name, 1000,
        time_begin, time_end, add_conditions={"quote": "USD", "code": currency})]


class DepthRow(object):
    '''
    深度数据
    '''

    def __lt__(self, another):
        return self.time < another.time


class TradeRow(DepthRow):

    DEPTH_TRADE = "_trade"
    DEPTH_TRADE = "_trade"
    DEPTH_FIRST = "s_first"

    def __init__(self, json_data, data_type, use_depth=DEPTH_TRADE):
        if "price_bid_trade" not in json_data:
            logging.info(json_data)
            self.bid = json_data["price_bid_trade_1000"] * json_data["huilv"]
            self.ask = json_data["price_ask_trade_1000"] * json_data["huilv"]
        else:
            self.bid = json_data["price_bid%s" % use_depth] * json_data["huilv"]
            self.ask = json_data["price_ask%s" % use_depth] * json_data["huilv"]
        self.time = datetime.datetime.fromtimestamp(json_data["time"] / 1000.0)
        self.data_type = data_type

    def debug_string(self):
        return "time:%s %s bid:%s ask:%s" % (self.time, self.data_type, self.bid, self.ask)

    @classmethod
    def get_spot(cls, token, quote, exchange, title, time_begin, time_end, use_depth):
        return sorted(cls(r, title, use_depth) for r in get_spot(token, time_begin, time_end, exchange, quote))

    @classmethod
    def get_future(cls, token, exchange, future_type, title, time_begin, time_end, use_depth):
        return sorted(cls(r, title, use_depth) for r in get_future(token, future_type, time_begin, time_end, exchange=exchange))

    @classmethod
    def get_spot_first(cls, token, quote, exchange, time_begin, time_end):
        return cls.get_spot(token, quote, exchange, exchange, time_begin, time_end, cls.DEPTH_FIRST)

    @classmethod
    def get_spot_trade(cls, token, quote, exchange, time_begin, time_end):
        return cls.get_spot(token, quote, exchange, exchange, time_begin, time_end, cls.DEPTH_TRADE)

    @classmethod
    def get_spot_mid(cls, token, quote, exchange, time_begin, time_end):
        return [(row.time, (row.bid + row.ask) / 2) for row in cls.get_spot_first(token, quote, exchange, time_begin, time_end)]

    @classmethod
    def get_spot_minute_average(cls, token, quote, exchange, time_begin, time_end):
        return continous_minute_average(cls.get_spot_mid(token, quote, exchange, time_begin - datetime.timedelta(minutes=1), time_end), time_begin, time_end)

    @classmethod
    def get_spot_minute_average_in_usd(cls, token, quote, exchange, time_begin, time_end):
        minute_dict = cls.get_spot_minute_average(token, quote, exchange, time_begin, time_end)
        quote_rate = get_currency_value(quote, time_begin, time_end)
        return {t: v * quote_rate[t] for t, v in minute_dict.items()}

    @classmethod
    def get_future_first(cls, token, exchange, future_type, time_begin, time_end):
        return cls.get_future(token, exchange, future_type, future_type, time_begin, time_end, cls.DEPTH_FIRST)

    @classmethod
    def get_future_trade(cls, token, exchange, future_type, time_begin, time_end):
        return cls.get_future(token, exchange, future_type, future_type, time_begin, time_end, cls.DEPTH_TRADE)

    @classmethod
    def get_future_mid(cls, token, exchange, future_type, time_begin, time_end):
        return [(row.time, (row.bid + row.ask) / 2) for row in cls.get_future_first(token, exchange, future_type, time_begin, time_end)]

    @classmethod
    def get_future_minute_average(cls, token, exchange, future_type, time_begin, time_end):
        return continous_minute_average(cls.get_future_mid(token, exchange, future_type, time_begin - datetime.timedelta(minutes=1), time_end), time_begin, time_end)


def get_trade_class(database, collection):
    class TradeData(Document):

        meta = {"db_alias": database, "collection": collection}

        time = IntField(primary_key=True)
        huilv = FloatField()
        price_bids_first = FloatField()
        price_asks_first = FloatField()
        price_bid_trade = FloatField()
        price_ask_trade = FloatField()

    return TradeData


def get_timeline_class(collection, alias="moving_average"):
    class TimeLine(Document):

        title = collection
        meta = {"db_alias": alias, "collection": collection}

        time = DateTimeField(primary_key=True)
        value = FloatField()

        @classmethod
        def insert_pair_list(cls, timeline):
            for t, v in timeline:
                cls(t, v).save()

        @classmethod
        def timeline(cls, time_begin, time_end):
            return [(row.time, row.value) for row in cls.objects(time__gte=time_begin, time__lt=time_end).order_by("time")]

        def __str__(self):
            return "<TimeLine-%s %s %s>" % (self.title, self.time, self.value)
    return TimeLine


def get_future_minute_class(exchange, token, future_type="quarter"):
    return get_timeline_class("_".join((exchange, token, future_type)))


OKEXQuarterMinute = {token: get_future_minute_class("okexft", token, "quarter") for token in ALL_OKEXFT_TOKENS}


def generate_future_spot_ma_data(exchange, token, spot_exchange, spot_quote, future_type, time_begin, time_end):
    get_future_minute_class(exchange, token, future_type).insert_pair_list(time_dict_rate(
        TradeRow.get_future_minute_average(token, exchange, future_type, time_begin, time_end),
        TradeRow.get_spot_minute_average_in_usd(token, spot_quote, spot_exchange, time_begin, time_end)).items())


def update_future_spot_ma_data(exchange, token, spot_exchange, spot_quote, future_type, until):
    data_class = get_future_minute_class(exchange, token, future_type)
    time_begin = last_updated(data_class)
    data_class.insert_pair_list(time_dict_rate(
        TradeRow.get_future_minute_average(token, exchange, future_type, time_begin, until),
        TradeRow.get_spot_minute_average_in_usd(token, spot_quote, spot_exchange, time_begin, until)).items())


def timeline_rate(timeline_a, timeline_b):
    assert(len(timeline_a) == len(timeline_b))
    return [(t1, v1 / v2) for (t1, v1), (t2, v2) in zip(timeline_a, timeline_b)]


def time_dict_rate(time_dict_a, time_dict_b):
    assert(len(time_dict_a) == len(time_dict_b))
    return {t1: v1 / v2 for (t1, v1), (t2, v2) in zip(sorted(time_dict_a.items()), sorted(time_dict_b.items()))}


def minute_average(time_line):
    minute_list = defaultdict(list)
    for t, v in time_line:
        minute_list[minute_ceil(t)].append(v)
    return sorted((t, numpy.mean(vl)) for t, vl in minute_list.items())


def continous_minute_average(time_line, time_begin, time_end):
    if not time_line:
        return None
    result = {k: v for k, v in minute_average(time_line) if k >= time_begin and k < time_end}
    fill_gaps(time_begin, time_end, result)
    return result


def time_weighted_moving_average(time_begin, time_end, half_life, price_list):
    '''
    生成时间加权的滑动平均
    '''
    result = {}
    ma_sum = ma_size = 0
    last_time = price_list[0][0]
    half_seconds = float(half_life.total_seconds())
    for t, mid in price_list:
        mul = 0.5 ** ((t - last_time).total_seconds() / half_seconds)
        ma_sum *= mul
        ma_sum += mid
        ma_size *= mul
        ma_size += 1
        last_time = t

        if t < time_begin:
            continue
        if t >= time_end:
            break
        minute = minute_ceil(t)
        result.setdefault(minute, ma_sum / ma_size)

    return result


def fill_gaps(time_begin, time_end, result):
    # 如果这一分钟没有值，用上一分钟的值
    step = datetime.timedelta(seconds=60)
    first_minute = truncate_minute(time_begin)
    first_exist = min(result)
    last = result[first_exist]
    if first_exist > first_minute:
        logging.warning("data missing from %s to %s", first_minute, first_exist)
    for t in datetime_range(first_minute, time_end + step, step):
        last = result.setdefault(t, last)


def get_usdt_ma(time_begin, time_end, half_life):
    '''
    生成 usdt 对美元汇率带时间衰减的平均
    '''
    data = TradeRow.get_spot_first("usdt", "usd", "kraken", time_begin - 5 * half_life, time_end + half_life)
    price_list = [(row.time, (row.bid + row.ask) / 2) for row in data]
    result = time_weighted_moving_average(time_begin - datetime.timedelta(minutes=1), time_end, half_life, price_list)

    fill_gaps(time_begin, time_end, result)
    return result


def get_currency_value(currency, time_begin, time_end):
    if currency == "usdt":
        return get_usdt_ma(time_begin, time_end, datetime.timedelta(seconds=initializer.args.usdt_half_life_seconds))
    currency = currency.upper()
    if currency in TWS_CURRENCY:
        return continous_minute_average(get_forex(currency, time_begin-datetime.timedelta(minutes=1), time_end), time_begin, time_end)
    if currency == "USD":
        return defaultdict(lambda: 1)
    raise ValueError("unknown currency %s", currency)


if __name__ == "__main__":
    initializer.init()
    for token in ALL_OKEXFT_TOKENS:
        update_future_spot_ma_data("okexft", token, "okex", "usdt", "quarter", truncate_minute(datetime.datetime.now()))
