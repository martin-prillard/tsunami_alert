# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Util functions
"""

"""
Transform longitude & latitude into (longitude,latitude)
"""
def calc_coord(row):
    row['coordinates']=(row['latitude'],row['longitude'])
    return row

"""
Calculate the coordinates of the centers of different GSM Zones
"""
def generate_GSM_zone_coordinates_CSV(data):
    GSM_Coord = data.groupby('GSM_code').mean()
    GSM_Coord = GSM_Coord.drop(['tel_num', 'timestamp','timeslot_cassandra'],axis=1)
    GSM_Coord = GSM_Coord.apply(lambda x : calc_coord(x), axis = 1)
    GSM_Coord.to_csv('../dataset/GSM_Coord.csv', encoding='utf-8')