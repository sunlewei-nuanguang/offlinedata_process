#encoding=utf-8

import math
import datetime
import numpy

from mongo_tools import register_connections  # noqa
from trade_row import TradeRow, ALL_OKEXFT_TOKENS
from mongoengine import IntField, FloatField, StringField, Document
from publib.initializer import initializer


class Spread(Document):

    meta = {"db_alias": "mechanical_model", "collection": "spread", "index": {"fields": [("#exchange", "#token", "#quote")], "unique": True}}

    id = IntField(primary_key=True)
    exchange = StringField(unique_with=["token", "quote"])
    token = StringField()
    quote = StringField()
    spread = FloatField()

    @classmethod
    def update_spot(cls, exchange, token, quote):
        now = datetime.datetime.now()
        spread = math.exp(numpy.mean([math.log(row.ask / row.bid - 1) for row in TradeRow.get_spot_trade(
            token, quote, exchange, now - datetime.timedelta(days=1), now) if row.bid < row.ask]))
        Spread.objects(exchange=exchange, token=token, quote=quote).update_one(upsert=True, set__spread=spread)

    @classmethod
    def get(cls, exchange, token, quote):
        ret = list(Spread.objects(exchange=exchange, token=token, quote=quote))
        assert(len(ret) == 1)
        return ret[0].spread


if __name__ == "__main__":
    initializer.init()
    for token in ALL_OKEXFT_TOKENS:
        Spread.update_spot("okex", token, "usdt")
