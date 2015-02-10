# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Alert SMS system
"""

import sys
sys.path.append('../util')
sys.path.append('../sgbd')
from util_time import timestamp_to_minute, minute_to_timeslot, str_timestamp_to_timestamp
from util_geo import get_GSM_codes_close_to_impact, get_node_close_to_impact_id
from util_aws import kill_instance
from cassandra_manager import get_phone_numbers
import redis_manager as rm
import datetime
import pandas as pd


"******************************************************************************************"
csv_gsm_coord = '../../dataset/GSM_Coord.csv'
csv_res_dir = '../../dataset/'
date_start = "2014-12-30 00:00:00,000"
date_end = "2015-02-01 00:00:00,000"
km_range = 500
cassandra_node = 5
"******************************************************************************************"


"""
Send SMS alert for each code_gsm's phone numbers
"""
def send_sms(GSM_code, phones_list):
    sms_GSM_code = pd.DataFrame(columns=['GSM_code','sending_time'])

    ts_gsm = datetime.datetime.now()

    sms_sent = 0

    for phone in phones_list:
        if (not rm.exists(phone)):
            ts = datetime.datetime.now()
            # redis insert
            rm.lpush(phone)
            rm.set(phone, ts)
            sms_sent = sms_sent + 1

    d = [GSM_code,ts_gsm]
    sms_GSM_code.loc[0]=d

    return sms_GSM_code, sms_sent


"""
Ask a yes/no question via raw_input() and return their answer
"""
def query_yes_no(question, default="yes"):
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


if __name__ == '__main__':

    replication = True

    # clean the last data in redis
    rm.cleanAll()

    total_number_sms_sent = 0
    nb_earthquake = 0

    while (replication):

        latitude = raw_input("\nTsunami latitude   : ")
        longitude = raw_input("Tsunami longitude  : ")
        tsunami_date = raw_input("Time of the impact : ")

        print '\nTSUNAMI : (' + str(latitude) + ', ' + str(longitude) + ') at ' + str(tsunami_date)

        print '\n------------------------------------------------------------------------------'
        instance_id, Closest_node = get_node_close_to_impact_id(latitude, longitude, km_range, nb_earthquake)
        instance = kill_instance(instance_id)
        print '------------------------------------------------------------------------------'
        print '\nOMG, the Tsunami destroyed ' + str(Closest_node) + "node (" + str(instance_id) + ') !'

        nb_earthquake = nb_earthquake + 1

        # get the time when the process is starting
        start_timer = datetime.datetime.now()

        print '\nAlert process started at ' + str(start_timer)

        time_start = timestamp_to_minute(date_start)
        time_step = timestamp_to_minute(date_end)
        t = minute_to_timeslot(timestamp_to_minute(tsunami_date), time_start, time_step)

        # get list of GSM codes near the epicentre
        code_gsm_list = get_GSM_codes_close_to_impact(latitude, longitude, km_range)['GSM_code'].tolist()

        print 'Number of GSM Zone to target : ' + str(len(code_gsm_list))

        total_sms_sent = pd.DataFrame(columns=['GSM_code','sending_time'])
        number_sms_sent = 0

        if (len(code_gsm_list) > 0):

            for code_gsm in code_gsm_list:
                try:
                    # get phone numbers (within code_gsm zone) to send SMS alert
                    phones_list = get_phone_numbers(code_gsm, t)
                    # send SMS alert
                    res_pd, sms_sent = send_sms(code_gsm, phones_list)
                    number_sms_sent += sms_sent
                    total_sms_sent = total_sms_sent.append(res_pd)
                except:
                    pass

            end_time = datetime.datetime.now()

            print 'Alert process completed at ' + str(end_time) + ' in ' + str(end_time-start_timer) + ' hours (' + str(number_sms_sent) + ' sent)'

            if (number_sms_sent > 0):
                print '\nPost-processing in progress to calculate time to reach 80 percents of the population...'

                # Cleaning the output DataFrame, with 'GSM_code','sending_hour','number_sms_sent','duration'
                GSM_Coord = pd.read_csv(csv_gsm_coord)
                total_sms_sent.reset_index(inplace=True, drop=False)
                total_sms_sent=total_sms_sent.merge(GSM_Coord,on='GSM_code')
                total_sms_sent['latitude']=total_sms_sent.coordinates.apply(lambda x: x.replace("\"","").replace("(","").replace(")","").split(",")[0])
                total_sms_sent['longitude']=total_sms_sent.coordinates.apply(lambda x: x.replace("\"","").replace("(","").replace(")","").split(",")[1])
                total_sms_sent=total_sms_sent[['GSM_code','latitude', 'longitude', 'sending_time']]
                # write result in csv file
                total_sms_sent.to_csv(csv_res_dir + "res_" + str(nb_earthquake) + ".csv")

                # -1 because there is the redis list
                x = int((rm.getDbSize() -total_number_sms_sent -1) * 0.2)
                time_80 = str_timestamp_to_timestamp(rm.get(rm.lindex(x)))

                print '80 percents of the population received the sms in ' + str(time_80-start_timer) + ' hours'
            else:
                total_number_sms_sent = total_number_sms_sent + number_sms_sent


            # if all node are down
            if (nb_earthquake >= cassandra_node):
                replication = False

        else:
            print '\nSorry, not for us.'

        # ask replication or not
        replication = query_yes_no("\nTsunami replication ?!")