# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Util time functions
"""

import time
from datetime import datetime


"******************************************************************************************"
date_format = '%Y-%m-%d %H:%M:%S,%f'
"******************************************************************************************"


"""
Convert timestamp to minutes
"""""
def timestamp_to_minute(date):
    return (long)((time.mktime(time.strptime(date, date_format))) / 60)


"""
Convert timestamp string to timestamp
"""
def str_timestamp_to_timestamp(date):
    return datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')


"""
From a date in minutes, return the right timeslot
"""
def minute_to_timeslot(date, t_start, time_step):
    timeslot = (date - t_start) // time_step
    if timeslot == 0:
        return timeslot * time_step + t_start
    else:
        # (q-1) because we want to contact phone numbers in the previous apart
        return (timeslot-1) * time_step + t_start
