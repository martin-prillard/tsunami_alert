# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Insert rows to Cassandra database
"""

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


# todo add pandas dataframe (Guillaume)
tel_by_codes_gsm = [['Yok_98', '323424', ['526198','232011']],
                    ['Yok_100', '23241', ['522440','232000']]]

# initialize
initialize()
# create the table from TsunamiModel
create_table_cassandra(TsunamiModel)
# insert row into cassandra
insert_to_cassandra(tel_by_codes_gsm)
print TsunamiModel.objects.count()
