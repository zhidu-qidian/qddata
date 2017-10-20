# coding: utf-8

from urllib import quote

from pymongo import MongoClient
from redis import from_url


REDIS_URL = "redis://内网IP:6379"
# REDIS_URL = "redis://127.0.0.1:6379"
REDIS_MAX_CONNECTIONS = 10

MONGODB_HOST_PORT = "内网IP:27017"
# MONGODB_HOST_PORT = "120.27.162.246:27017"
MONGODB_USER = "third"
MONGODB_PASSWORD = ""


def get_mongodb_database(database):
    url = "mongodb://{0}:{1}@{2}/{3}".format(
        MONGODB_USER, quote(MONGODB_PASSWORD), MONGODB_HOST_PORT, database
    )
    client = MongoClient(host=url, maxPoolSize=1, minPoolSize=1)
    return client.get_default_database()


def get_cache_client(db):
    return from_url(REDIS_URL, db=db, max_connections=REDIS_MAX_CONNECTIONS)


redis = get_cache_client(2)
db = get_mongodb_database("thirdparty")
