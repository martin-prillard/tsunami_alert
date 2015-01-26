# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Cassandra manager
"""

from cqlengine.management import sync_table
from cqlengine import connection


# setup the connection to our cassandra server(s) and the default keyspace
connection.setup(['127.0.0.1'], 'tsunami_project')


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