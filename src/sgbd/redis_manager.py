# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Redis manager
"""

import redis

# initialize redis
r = redis.StrictRedis(host='localhost', port=6379, db=0) # todo replace by redis cluster ?

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