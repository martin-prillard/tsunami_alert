# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Send alert sms
"""

from cqlengine import connection
import datetime
import time
import redis
from tsunami import TsunamiModel


"""
Initialize
"""
def initialize():
    # setup the connection to our cassandra server(s) and the default keyspace
    connection.setup(['127.0.0.1'], 'tsunami_project')
    # initialize redis
    r = redis.StrictRedis(host='localhost', port=6379, db=0) # todo replace by redis cluster ?

"""
Get the phone numbers to send them SMS alert for each code_gsm
"""
def get_phone_numbers():
    phones_list = []
    for code_gsm in code_gsm_list:
        phones_list = phones_list + TsunamiModel.filter(code_gsm=code_gsm, t=t).limit(1)[0].tel
    return phones_list

"""
Send SMS alert for each code_gsm's phone numbers
"""
def send_sms(phones_list):
    percent = 0
    total_phones = len(phones_list)
    phones_sms = {}
    for phone in phones_list:
        if (percent < 80):
            ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            phones_sms[str(phone)] = str(ts)
            percent = (len(phones_sms) / total_phones) * 100
        else:
            break
    return phones_sms, percent

"""
Display results
"""
def insert_to_redis(phones_sms):
    for key in phones_sms.keys():
        print 'SMS alert sended at ' + phones_sms[key] + ' to ' + key
        # stock in Redis phone numbers which received SMS alert
        # r.set(str(phone), ts) # todo test it on linux


# todo get the code_gsm list (Guillaume)
code_gsm_list = ['Yok_98']
# todo get the time of earthquake
t = '323424'

# initialize
initialize()
# get phone numbers to send
phones_list = get_phone_numbers()
# send SMS alert
phones_sms, percent = send_sms(phones_list)
# insert result in redis
insert_to_redis(phones_sms)
print str(percent) + '%'