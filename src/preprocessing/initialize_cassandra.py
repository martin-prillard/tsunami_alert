# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Initialize the database
"""

import sys
sys.path.append('../sgbd')
from cassandra_manager import create_table_cassandra
from cqlengine import columns
from cqlengine.models import Model


# mapper object with Cassandra model
class TsunamiModel(Model):
    code_gsm = columns.Text(primary_key=True)
    t        = columns.BigInt(primary_key=True)
    tel      = columns.List(columns.Integer)


if __name__ == '__main__':
    # create the database TsunamiModel
    create_table_cassandra(TsunamiModel)