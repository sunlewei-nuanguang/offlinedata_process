#encoding=utf-8

import logging
import datetime
import numpy
import pandas
import simplejson

from mongo_tools import register_connections, last_updated, get_redis_client, get_latest  # noqa
from scipy.stats import norm
from scipy.optimize import lsq_linear
from mongoengine import FloatField, DateTimeField, ListField, Document

from initializer import initializer
from utils import truncate_minute
from trade_row import ALL_OKEXFT_TOKENS, OKEXQuarterMinute, timeline_rate, update_future_spot_ma_data


WINDOW_LIST = [10 * (2 ** i) for i in range(11)]
TRAIN_WAIT_LIST = [2 ** i for i in range(13)]

redis_client = get_redis_client()


def get_ma_class(collection, window_list):
    class MovingAverage(Document):

        title = collection
        window = sorted(window_list)
        redis_key_for_latest = "moving_average_%s_latest" % collection

        meta = {"db_alias": "moving_average", "collection": collection}

        time = DateTimeField(primary_key=True)
        ma = ListField(FloatField())
        value = FloatField()

        @classmethod
        def generate_from_timeline(cls, timeline, time_begin, time_end):
            value_list = pandas.Series([v for t, v in timeline])
            window_ma = {w: value_list.rolling(w, min_periods=1).mean() for w in cls.window}
            assert(timeline[0][0] <= time_begin)
            assert(timeline[-1][0] >= time_end - datetime.timedelta(minutes=1))
            for index, (t, v) in enumerate(timeline):
                if time_begin <= t < time_end:
                    cls(time=t, value=v, ma=[window_ma[w][index] for w in cls.window]).save()
            redis_client.set(cls.redis_key_for_latest, simplejson.dumps(get_latest(cls).ma), ex=90)

        @classmethod
        def get_prediction(cls, wait_model, time_begin, time_end):
            data = list(cls.objects(time__gte=time_begin, time__lt=time_end).order_by("time"))
            window_diff = numpy.array([[r.value / r.ma[win_index] - 1 for r in data] for win_index, w in enumerate(cls.window)])
            return numpy.matmul(wait_model["reversion"], window_diff)

        @classmethod
        def test_reversion(cls, model, time_begin, time_end):
            data = list(cls.objects(time__gte=time_begin, time__lt=time_end).order_by("time"))
            window_diff = numpy.array([[r.value / r.ma[win_index] - 1 for r in data] for win_index, w in enumerate(cls.window)])
            for wait in TRAIN_WAIT_LIST:
                wait_model = model.get(str(wait))
                if not wait_model:
                    wait_model = model[wait]
                logging.info("%s wait:%s %s", cls.title, wait, wait_model)
                label = numpy.array([after.value / before.value - 1 for before, after in zip(data, data[wait:])])
                predict = sum(alpha * diff[:-wait] for alpha, diff in zip(wait_model["reversion"], window_diff)) * 0.5
                label_len = numpy.linalg.norm(label)
                after_len = numpy.linalg.norm(label + predict)
                logging.info("%s wait:%s before:%s / after:%s = %s", cls.title, wait, label_len, after_len, after_len / label_len)

        @classmethod
        def generate_reversion(cls, time_begin, time_end):
            data = list(cls.objects(time__gte=time_begin, time__lt=time_end).order_by("time"))
            model = {}
            window_diff = numpy.array([[r.value / r.ma[win_index] - 1 for r in data] for win_index, w in enumerate(cls.window)])
            for wait in TRAIN_WAIT_LIST:
                label = numpy.array([after.value / before.value - 1 for before, after in zip(data, data[wait:])])
                local_diff = window_diff[:, :-wait]
                window_weight = lsq_linear(-local_diff.transpose(), label, bounds=(0, 1)).x
                label_diff = window_diff[:, wait:]
                local_predict = numpy.matmul(window_weight, local_diff)
                label_predict = numpy.matmul(window_weight, label_diff)
                remain = numpy.dot(local_predict, label_predict) / (numpy.linalg.norm(local_predict) ** 2)
                _, std = norm.fit(label_predict - local_predict * remain)
                model[wait] = {"reversion": list(window_weight), "std": std, "remain": remain}
            cls.test_reversion(model, time_begin, time_end)
            return model

        @classmethod
        def time_dict(cls, time_begin, time_end):
            return {o.time: o for o in cls.objects(time__gte=time_begin, time__lt=time_end)}

        @classmethod
        def latest(cls):
            return cls(ma=simplejson.loads(redis_client.get(cls.redis_key_for_latest)))

        def numpy_ma(self):
            try:
                return self._numpy_ma
            except AttributeError:
                self._numpy_ma = numpy.array(self.ma)
                return self._numpy_ma

        def calculate_offset(self, mid, wait_model):
            return numpy.matmul(mid - self.numpy_ma(), wait_model["reversion"])

    return MovingAverage


OKEXBTCQuarterSpot = get_ma_class("okexft_btc_quarter_spot", WINDOW_LIST)
OKEXTokenQuarter = {token: get_ma_class("okexft_%s_to_btc_quarter" % token, WINDOW_LIST) for token in ALL_OKEXFT_TOKENS}


def get_okexft_token_diff_timeline(token, time_begin, time_end):
    return timeline_rate(
        OKEXQuarterMinute[token].timeline(time_begin, time_end),
        OKEXQuarterMinute["btc"].timeline(time_begin, time_end))


def get_year_all_quarters(year):
    settle_time = datetime.time(16, 0)
    for month, day in [(3, 31), (6, 30), (9, 30), (12, 31)]:
        date = datetime.date(year, month, day)
        while date.weekday() != 4:
            date -= datetime.timedelta(1)
        yield datetime.datetime.combine(date, settle_time)


def last_settle(t):
    for settle_time in reversed(list(get_year_all_quarters(t.year - 1)) + list(get_year_all_quarters(t.year))):
        if settle_time < t:
            return settle_time


def generate_all_future_spot_ma_data(time_begin, time_end):
    settle_time = last_settle(time_end)
    if settle_time > time_begin:
        generate_all_future_spot_ma_data(time_begin, settle_time)
        generate_all_future_spot_ma_data(settle_time, time_end)
        return
    timeline_begin = max(settle_time, time_begin - datetime.timedelta(minutes=WINDOW_LIST[-1]-1))

    for token in ALL_OKEXFT_TOKENS:
        if token == "btc":
            OKEXBTCQuarterSpot.generate_from_timeline(
                OKEXQuarterMinute["btc"].timeline(timeline_begin, time_end), time_begin, time_end)
        else:
            OKEXTokenQuarter[token].generate_from_timeline(
                get_okexft_token_diff_timeline(token, timeline_begin, time_end), time_begin, time_end)


if __name__ == "__main__":
    initializer.init()
    now = truncate_minute(datetime.datetime.now())
    logging.info("update until %s", now)
    for token in ALL_OKEXFT_TOKENS:
        update_future_spot_ma_data("okexft", token, "okex", "usdt", "quarter", now)
    last = {token: last_updated(OKEXBTCQuarterSpot if token == "btc" else OKEXTokenQuarter[token]) for token in ALL_OKEXFT_TOKENS}
    logging.info(last)
    generate_all_future_spot_ma_data(min(last.values()), now)
