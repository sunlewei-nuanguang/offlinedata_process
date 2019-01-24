#encoding=utf-8

import redis

from mongoengine import register_connection
from conf.publib_conf import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD


def register_connections():
    register_connection("moving_average", db="moving_average", authentication_source="admin", host="172.31.140.86", port=27888, username="adminall", password="moveall")
    register_connection("okexft", db="okexft", authentication_source="admin", host="172.31.140.86", port=28888, username="adminall", password="moveall")
    register_connection("mechanical_model", db="mechanical_model", authentication_source="admin", host="172.31.140.86", port=27888, username="adminall", password="moveall")


def get_latest(doc_class):
    return doc_class.objects.order_by("-time").first()


def last_updated(doc_class):
    return get_latest(doc_class).time


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, password=REDIS_PASSWORD)


register_connections()
