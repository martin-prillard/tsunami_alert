# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Tsunami model
"""

from cqlengine import columns
from cqlengine.models import Model

# mapper object with Cassandra model
class TsunamiModel(Model):
    code_gsm = columns.Text(primary_key=True)
    t        = columns.BigInt(primary_key=True)
    tel      = columns.List(columns.Text)