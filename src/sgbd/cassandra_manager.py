# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Cassandra manager
"""

from cqlengine import columns
from cqlengine.models import Model
from cqlengine.management import sync_table
from cqlengine import connection
from cqlengine import BatchQuery


# setup the connection to our cassandra server(s) and the default keyspace
connection.setup(['127.0.0.1'], 'tsunami_project')


# mapper object with Cassandra model
class TsunamiModel(Model):
    code_gsm = columns.Text(primary_key=True)
    t        = columns.BigInt(primary_key=True)
    tel      = columns.List(columns.Integer)


"""
Create our CQL table in Cassandra
"""
def create_table_cassandra(model):
    sync_table(model)


"""
Get the phone numbers to send them SMS alert for each code_gsm
"""
def get_phone_numbers(code_gsm_list, t):
    phones_list = []
    for code_gsm in code_gsm_list:
        try:
            phones_list = phones_list + TsunamiModel.filter(code_gsm=code_gsm, t=t).limit(1)[0].tel
        except:
             pass
    return phones_list


"""
Insert rows into Cassandra table
"""
def insert_to_cassandra(tel_by_codes_gsm):
    # bulk size for import in Cassandra
    size_bulk = 500 #todo bulk
    # now we can insert rows using a context manager
    with BatchQuery() as b:
        # for each line of the array, insert it here
        for tel_by_code_gsm in tel_by_codes_gsm:
            em1 = TsunamiModel.batch(b).create(code_gsm=tel_by_code_gsm[0],
                                               t=tel_by_code_gsm[1],
                                               tel=tel_by_code_gsm[2])