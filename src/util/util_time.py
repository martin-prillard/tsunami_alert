# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Util time functions
"""

import time


"""
Convert timestamp to minutes
"""""
def timestamp_to_minute(date):
    fmt = '%Y-%m-%d %H:%M:%S.%f'
    return (long)((time.mktime(time.strptime(date, fmt))) / 60)


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