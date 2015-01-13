# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Util time functions
"""

import time


"""
Convert timestamp to seconds
"""""
def timestamp_to_second(x, fmt):
    return (long)(time.mktime(time.strptime(x, fmt)))


"""
From a timestamp, return a timeslot
"""
def get_timeslot(t, time_step, t_start):
    q = (t - t_start) // time_step
    if q == 0:
        return q * time_step + t_start
    else:
        # (q-1) because we want to contact phone numbers in the previous apart
        return (q-1) * time_step + t_start