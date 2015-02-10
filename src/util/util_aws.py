# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Util AWS functions
"""

from boto.ec2.connection import EC2Connection


"******************************************************************************************"
IMAGE           = 'ami-ada2b6c4' # Basic 64-bit Amazon Linux AMI
KEY_NAME        = 'your_pem_file'
INSTANCE_TYPE   = 'm3.xlarge'
ZONE            = 'us-east-1d' # Availability zone must match
SECURITY_GROUPS = ['TESTING_TSUNAMI_0302'] # Security group allows SSH
AWS_KEY         = 'your_AWS_key'
AWS_SECRET_KEY  = 'your_secret_key'
"******************************************************************************************"


"""
Kill the instance (wiid of the instance)
"""
def kill_instance(instance_id):

    instance_to_kill = instance_id

    # Connection to EC2
    conn = EC2Connection(AWS_KEY, AWS_SECRET_KEY)

    # get the instance to kill (input = instance_id)
    instance = conn.get_only_instances(instance_to_kill)
    #kill the corresponding instance

    instance[0].terminate()

    return instance
