# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Alert SMS system
"""

import sys
sys.path.append('../util')
sys.path.append('../sgbd')
from cqlengine import connection
from util_time import timestamp_to_second
from util_geo import get_GSM_codes_close_to_impact
from cassandra_manager import get_phone_numbers
import redis_manager as r
import datetime


"""
Send SMS alert for each code_gsm's phone numbers
"""
def send_sms(phones_list):
    phones_sms = {}
    for phone in phones_list: #todo check, numero unique
        ts = datetime.datetime.now()
        phones_sms[str(phone)] = str(ts)
        # stock in Redis phone numbers which received SMS alert
        # r.set(str(phone), ts) # todo test it on linux
        print 'SMS alert sent at ' + str(ts) + ' to ' + str(phone)
    return phones_sms


"""
Calcul time to send SMS at 80 percent of the population
"""
def calculate_process_time(phones_sms):
    percent = 0
    cpt = 0
    total_phones = len(phones_sms)
    start = 0
    first = True
    fmt = '%Y-%m-%d %H:%M:%S.%f'
    for key in phones_sms.keys():
        if (percent < 80):
            # fist input
            if (first):
                start = timestamp_to_second(phones_sms[key], fmt)
                stop = timestamp_to_second(phones_sms[key], fmt)
                first = False
            cpt = cpt + 1
            percent = (cpt / total_phones) * 100
        else:
            stop = timestamp_to_second(phones_sms[key], fmt)
            break
    return stop - start


if __name__ == '__main__':
    km_range = 100
    # todo raw_input
    latitude = 35.557485
    longitude = 136.907394
    t = '1420616296'

    # get list of GSM codes near the epicentre
    code_gsm_list = get_GSM_codes_close_to_impact(latitude, longitude, km_range)['GSM_code'].tolist()
    # get phone numbers to send
    phones_list = get_phone_numbers(code_gsm_list, t)
    # send SMS alert
    phones_sms = send_sms(phones_list)
    # calculate process time
    print calculate_process_time(phones_sms)