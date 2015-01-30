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
"******************************************************************************************"


# initialize redis
r = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)


"""
Put element in redis
"""
def set(key, value):
    r.set(key, value)


"""
Get element from redis
"""
def get(key):
    return r.get(key)


"""
Removes data from ALL databases
"""
def cleanAll():
    r.flushall()