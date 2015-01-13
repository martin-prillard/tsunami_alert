# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Insert rows to Cassandra database
"""

import pandas as pd
import time
import numpy as np
from cqlengine import connection
from cqlengine import BatchQuery
from cqlengine.management import sync_table
from tsunami import TsunamiModel

"""
Initialize
"""
def initialize():
    # setup the connection to our cassandra server(s) and the default keyspace
    connection.setup(['127.0.0.1'], 'tsunami_project')


"""
Create our CQL table in Cassandra
"""
def create_table_cassandra(model):
    sync_table(model)

"""
Insert rows into Cassandra table
"""
def insert_to_cassandra(tel_by_codes_gsm):
    # bulk size for import in Cassandra
    size_bulk = 500 #todo
    # now we can insert rows using a context manager
    with BatchQuery() as b:
        # for each line of the array, insert it here
        for tel_by_code_gsm in tel_by_codes_gsm:
            em1 = TsunamiModel.batch(b).create(code_gsm=tel_by_code_gsm[0],
                                               t=tel_by_code_gsm[1],
                                               tel=tel_by_code_gsm[2])


time_step=60*10

# Import original data
data = pd.read_csv('../dataset/data_tsunami.csv',sep=';',index_col=None, header=None)
data.columns = ['date','GSM_code','latitude','longitude','tel_num']
data = data.sort(['GSM_code','date'],ascending=[1,0])
data = data.reset_index(drop=True)

def timestamp(x):
    # x en format donné du csv
    # renvoie le timestamp en seconde de la date
    fmt = '%Y-%m-%d %H:%M:%S,%f'
    return (long)(time.mktime(time.strptime(x, fmt)))

def colum_name(t,time_step,tdeb):
    # tous les temps doivent être en timestamp seconde
    # convient après la sortie de la fonction totimestamp
    q=(t-tdeb)//time_step
    if q == 0:
        return q*time_step+tdeb
    else:
    # (q-1) car on souhaite contacter les numéros dans l'intervalle précédent
        return (q-1)*time_step+tdeb

data['timestamp'] = data['date'].apply(lambda x :timestamp(x))
tdeb=min(data['timestamp'])
tfin = max(data['timestamp'])
col=range(tdeb,tfin,time_step)
data['timeslot_cassandra']=data['timestamp'].apply(lambda x:colum_name(x,time_step,tdeb))

data = data.groupby(['GSM_code','timeslot_cassandra'])

print ("\n output pour intégration dans cassandra")
tel_by_codes_gsm = []
for name, group in data:
    tel_num = group['tel_num'].tolist()
    output = [name[0], name[1], tel_num]
    tel_by_codes_gsm.append(output)

print tel_by_codes_gsm

# initialize
initialize()
# create the table from TsunamiModel
create_table_cassandra(TsunamiModel)
# insert row into cassandra
insert_to_cassandra(tel_by_codes_gsm)
print TsunamiModel.objects.count()
