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
import pandas as pd
import numpy as np
from pandas import DataFrame, Series
from geopy.distance import vincenty
import datetime

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
def get_phone_numbers(t):
    phones_list = []
    for code_gsm in code_gsm_list:
        try:
            phones_list = phones_list + TsunamiModel.filter(code_gsm=code_gsm, t=t).limit(1)[0].tel
        except:
             pass
    return phones_list

"""
Send SMS alert for each code_gsm's phone numbers
"""
def send_sms(phones_list):
    percent = 0
    cpt = 0
    total_phones = len(phones_list)
    phones_sms = {}
    for phone in phones_list: #todo check, numero unique
        if (percent < 80):
            ts = datetime.datetime.now()
            phones_sms[str(phone)] = str(ts)
            cpt = cpt + 1
            percent = (cpt / total_phones) * 100
        else:
            break
    return phones_sms, percent

"""
Display results
"""
def insert_to_redis(phones_sms):
    for key in phones_sms.keys():
        print 'SMS alert sent at ' + phones_sms[key] + ' to ' + key
        # stock in Redis phone numbers which received SMS alert
        # r.set(str(phone), ts) # todo test it on linux


km_range=100
latitude = 35.557485
longitude = 136.907394

#Calculate distance between seism and a given coordinate
def cal_distance_to_seism(row, impact_coordinates):
    row['dist_from_seism']=vincenty(row['coordinates'],impact_coordinates).miles
    return row

def select_GSM_according_to_distance_to_impact():
    GSMZone_dist = pd.read_csv('../dataset/GSM_Coord.csv')
    impact_coord = (latitude,longitude)
    GSMZone_dist = GSMZone_dist.apply(lambda x:cal_distance_to_seism(x,impact_coord), axis=1)
    GSMZone_dist = GSMZone_dist.sort(['dist_from_seism'],ascending=1)
    GSMZone_dist_500 = GSMZone_dist[GSMZone_dist['dist_from_seism']<=km_range]
    return GSMZone_dist_500

# todo get the code_gsm list (Guillaume)

code_gsm_list = select_GSM_according_to_distance_to_impact()
code_gsm_list = code_gsm_list['GSM_code'].tolist()
print code_gsm_list

# todo get the time of earthquake
t = '1420616296'

# initialize
initialize()
# get phone numbers to send
phones_list = get_phone_numbers(t)
# send SMS alert
phones_sms, percent = send_sms(phones_list)
# insert result in redis
insert_to_redis(phones_sms)
print str(percent) + '%'