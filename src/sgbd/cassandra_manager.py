# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Cassandra manager
"""

from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection


"******************************************************************************************"
cassandra_cluster_ip = ['127.0.0.1']
cassandra_keyspace = 'tsunami_project'
"******************************************************************************************"


# setup the connection to our cassandra server(s) and the default keyspace
connection.setup(cassandra_cluster_ip, cassandra_keyspace)


# mapper object with Cassandra model
class tsunami_table(Model):
    code_gsm = columns.Text(primary_key=True)
    timeslot        = columns.Integer(primary_key=True)
    phone      = columns.List(columns.Integer)


"""
Get the phone numbers to send them SMS alert for each code_gsm
"""
def get_phone_numbers(code_gsm, timeslot):
    phones_list = []
    try:
        phones_list = tsunami_table.filter(code_gsm=code_gsm, timeslot=timeslot).limit(1)[0].phone
    except:
         pass
    # print phones_list # TODO test only
    return phones_list