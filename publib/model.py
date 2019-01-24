#encoding=utf-8

import datetime
import simplejson

from mongoengine import DateTimeField, StringField, Document
from mongo_tools import register_connections  # noqa


class Model(Document):

    meta = {"db_alias": "mechanical_model", "collection": "model"}

    key = StringField(primary_key=True)
    value = StringField()
    update_time = DateTimeField(default=datetime.datetime.now)

    @classmethod
    def get(cls, k):
        ret = cls.objects(key=k)
        assert(ret)
        return simplejson.loads(ret[0].value)
