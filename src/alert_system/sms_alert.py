# -*- coding: utf8 -*-
from __future__ import unicode_literals
__author__ = 'tsunami_team'
"""
Alert SMS system
"""

import sys
sys.path.append('../util')
sys.path.append('../sgbd')
from util_time import timestamp_to_minute, minute_to_timeslot
from util_geo import get_GSM_codes_close_to_impact
from cassandra_manager import get_phone_numbers
import datetime
import pandas as pd


"******************************************************************************************"
csv_gsm_coord = '../../dataset/GSM_Coord.csv'
csv_res = '../../dataset/res.csv'
date_start = "2014-12-30 00:00:00,000"
date_end = "2015-02-01 00:00:00,000"
km_range = 500
"******************************************************************************************"


"""
Send SMS alert for each code_gsm's phone numbers
"""
def send_sms(GSM_code, phones_list):
    sms_GSM_code = pd.DataFrame(columns=['GSM_code','tel_num','sending_time'])
    
    for phone in phones_list:
        ts = datetime.datetime.now()
        d = [GSM_code,phone,ts]
        sms_GSM_code.loc[len(sms_GSM_code)+1]=d
    return sms_GSM_code


def calculate_80_percent_time(total_sms_sent, start_time):
    catch = False
    for (index, row) in total_sms_sent.iterrows():
        if catch == False:
            if row['percentage_sent']<0.8:
                time = row['sending_time']
            else:
                catch = True
    return time


if __name__ == '__main__':
    # TODO enable it in production
    latitude = raw_input("\nTsunami latitude   : ")
    longitude = raw_input("Tsunami longitude  : ")
    tsunami_date = raw_input("Time of the impact : ")

    print '\nTSUNAMI : (' + str(latitude) + ', ' + str(longitude) + ') at ' + str(tsunami_date)

    # get the time when the process is starting
    start_timer = datetime.datetime.now()

    print '\nAlert process started at ' + str(start_timer)

    time_start = timestamp_to_minute(date_start)
    time_step = timestamp_to_minute(date_end)
    t = minute_to_timeslot(timestamp_to_minute(tsunami_date), time_start, time_step)

    # get list of GSM codes near the epicentre
    code_gsm_list = get_GSM_codes_close_to_impact(latitude, longitude, km_range)['GSM_code'].tolist()

    print 'Number of GSM Zone to target : ' + str(len(code_gsm_list))

    total_sms_sent = pd.DataFrame(columns=['GSM_code','tel_num'])
    number_sms_sent = 0

    if (len(code_gsm_list) > 0):

        for code_gsm in code_gsm_list:
            try:
                # get phone numbers (within code_gsm zone) to send SMS alert
                phones_list = get_phone_numbers(code_gsm, t)
                number_sms_sent += len(phones_list)
                # send SMS alert
                sms_sent_GSM_zone = send_sms(code_gsm, phones_list)
                total_sms_sent = total_sms_sent.append(sms_sent_GSM_zone)
            except:
                pass

        end_time = datetime.datetime.now()

        print 'Alert process completed at ' + str(end_time) + ' in ' + str(end_time-start_timer) + ' hours (' + str(number_sms_sent) + ' sent)'
        print '\nPost-processing in progress to calculate time to reach 80 percents of the population...'

        # Cleaning the output DataFrame, with 'GSM_code','sending_hour','number_sms_sent','duration'
        GSM_Coord = pd.read_csv(csv_gsm_coord)

        #total_sms_sent.reset_index(inplace=True, drop=True)
        #calculate the sendig duration for each timezone
        total_sms_sent['duration'] = total_sms_sent.apply(lambda x : total_sms_sent[total_sms_sent['GSM_code']==x['GSM_code']]['sending_time'].max()-total_sms_sent[total_sms_sent['GSM_code']==x['GSM_code']]['sending_time'].min(), axis=1)
        #calculate the sendig time for each timezone
        total_sms_sent['number_sms_sent']=total_sms_sent.apply(lambda x : total_sms_sent.groupby('GSM_code').count()['tel_num'][x['GSM_code']],axis=1)
        total_sms_sent = total_sms_sent.groupby('GSM_code').first()
        total_sms_sent.reset_index(inplace=True, drop=False)
        total_sms_sent=total_sms_sent.merge(GSM_Coord,on='GSM_code')
        total_sms_sent['cum_sum_sms']=total_sms_sent['number_sms_sent'].cumsum()
        total_sms_sent['percentage_sent']=total_sms_sent['cum_sum_sms']/total_sms_sent['number_sms_sent'].sum()
        total_sms_sent=total_sms_sent[['GSM_code','sending_time','number_sms_sent','cum_sum_sms','percentage_sent','duration']]
        # write result in csv file
        total_sms_sent.to_csv(csv_res)

        # calculate and display process time
        time_80 = calculate_80_percent_time(total_sms_sent,start_timer)

        print '80 percents of the population received the sms in ' + str(time_80-start_timer) + ' hours'

    else:
        print '\nSorry, not for us.'