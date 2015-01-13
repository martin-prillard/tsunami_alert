# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Insert rows to Cassandra database
"""

import sys
sys.path.append('../util')
sys.path.append('../sgbd')
import pandas as pd
from util_time import timestamp_to_second, get_timeslot
from util_geo import generate_GSM_zone_coordinates_CSV
import cassandra_manager as cas


"""
Read and process csv file
"""
def process_csv():
    time_step=60*10
    # Import original data
    data = pd.read_csv('../../dataset/data_tsunami.csv',sep=';',index_col=None, header=None)
    data.columns = ['date','GSM_code','latitude','longitude','tel_num']
    data = data.sort(['GSM_code','date'],ascending=[1,0])
    data = data.reset_index(drop=True)
    # convert timestamp to second
    data['timestamp'] = data['date'].apply(lambda x :timestamp_to_second(x, '%Y-%m-%d %H:%M:%S,%f'))
    t_start = min(data['timestamp'])
    t_end = max(data['timestamp'])
    col = range(t_start,t_end,time_step)
    # add period of time
    data['timeslot_cassandra'] = data['timestamp'].apply(lambda x : get_timeslot(x,time_step,t_start))

    # Calculate the coordinates of the centers of different GSM Zones in a CSV file
    generate_GSM_zone_coordinates_CSV(data)

    # group by gsm code and timeslot
    data = data.groupby(['GSM_code','timeslot_cassandra'])
    return data


"""
Return a list of row which contains
all phone numbers near their gsm
for each period of time
"""
def get_tel_by_codes_gsm(data):
    tel_by_codes_gsm = []
    for name, group in data:
        tel_num = group['tel_num'].tolist()
        output = [name[0], name[1], tel_num]
        tel_by_codes_gsm.append(output)
    return tel_by_codes_gsm


if __name__ == '__main__':
    # read and treat csv file
    data = process_csv()
    # create the table from TsunamiModel
    cas.create_table_cassandra(cas.TsunamiModel)
    # insert row into cassandra
    tel_by_codes_gsm = get_tel_by_codes_gsm(data)
    cas.insert_to_cassandra(tel_by_codes_gsm)

    print cas.TsunamiModel.objects.count()
