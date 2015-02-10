# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Redis manager
"""

import redis


"******************************************************************************************"
redis_ip = '127.0.0.1'
redis_port = 6379
redis_list = 'phoneList'
"******************************************************************************************"


# initialize redis
r = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)


"""
Put element in redis
"""
def set(key, value):
    r.set(key, value)


"""
Push an element into a list
"""
def lpush(value):
    r.lpush(redis_list, value)


"""
Get element from redis
"""
def get(key):
    return r.get(key)


"""
Get element from list
"""
def lindex(value):
    return r.lindex(redis_list, value)


"""
Return true if the key exist
"""
def exists(key):
    if (r.exists(key) == 1):
       return True
    else:
       return False


"""
Removes data from ALL databases
"""
def cleanAll():
    r.flushall()


"""
Return db size
"""
def getDbSize():
    return r.dbsize()

