# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
TODO
"""

import sys
sys.path.append('../util')
sys.path.append('../sgbd')
import pandas as pd
from util_time import timestamp_to_second, get_timeslot
"""
from pyspark import SparkContext

# cluster initialization
SparkContext.setSystemProperty('spark.executor.memory', '20g')
if 'sc' not in globals():
    sc = SparkContext( CLUSTER_URL, 'pyspark')

input_file = '../../dataset/data_tsunami_sorted.csv'

# 0782675123 ((0782675123, 'yok_23', 23/11/1990),
#             (0782675123, 'yok_23', 24/11/1990),
#             (0782675123, 'yok_24', 25/11/1990)...
csv = sc.textFile(input_file) # depuis AWS : sc.textFile("s3://...")
by_phone_sorted_by_date = csv.map(_.split(";"))\
    .map(lambda v: (v(4), v(1), timestamp_to_second(v(0), '%Y-%m-%d %H:%M:%S,%f')))\
    .sortBy(_._3)\
    .groupBy(_._1)\
    .flatMap(lambda v: v._2)


print by_phone_sorted_by_date

def todo():
"""

data = pd.read_csv('../../dataset/data_tsunami_sorted.csv',sep=';',index_col=None, header=None)
data.columns = ['tel_num','GSM_code','date']
# convert timestamp to second
data['timestamp'] = data['date'].apply(lambda x :timestamp_to_second(x, '%Y-%m-%d %H:%M:%S,%f'))

# phone;GSM_code;start;end
output = pd.DataFrame() #todo use it
now = max(data['timestamp'])
start_temp = None
phone_temp = None
gsm_code_temp = None

for i in range(0, len(data)):

    phone = data['tel_num'][i]
    gsm_code = data['GSM_code'][i]
    start = data['timestamp'][i]

    # first row read, initialization
    if(phone_temp == None and gsm_code_temp == None and start_temp == None):
        phone_temp = phone
        gsm_code_temp = gsm_code
        start_temp = start
    # elif the user has changed his location
    if(phone == phone_temp and gsm_code != gsm_code_temp):
        #output.append(phone, gsm_code_temp, start_temp, start)
        print str(phone) + ' ' + str(gsm_code_temp) + ' ' + str(start_temp) + ' ' + str(start)
        gsm_code_temp = gsm_code
        start_temp = start
    # elif another users
    elif(phone != phone_temp):
        # we keep last position known of the last user
        #output.append(phone_temp, gsm_code_temp, start_temp, now)
        print str(phone_temp) + ' ' + str(gsm_code_temp) + ' ' + str(start_temp) + ' ' + str(now)
        phone_temp = phone
        gsm_code_temp = gsm_code
        start_temp = start
    # last row read, close the end value for the last user
    elif(i == len(data)):
        #output.append(phone_temp, gsm_code_temp, start_temp, now)
        print str(phone_temp) + ' ' + str(gsm_code_temp) + ' ' + str(start_temp) + ' ' + str(now)


# save in csv file
# todo
