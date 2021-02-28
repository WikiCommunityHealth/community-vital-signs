# -*- coding: utf-8 -*-

# script
import wikilanguages_utils
from wikilanguages_utils import *
# time
import time
import datetime
from dateutil import relativedelta
import calendar
# system
import os
import sys
import shutil
import re
import random
import operator
# databases
import MySQLdb as mdb, MySQLdb.cursors as mdb_cursors
import sqlite3
# files
import gzip
import zipfile
import bz2
import json
import csv
import codecs
# requests and others
import requests
import urllib
import webbrowser
import reverse_geocoder as rg
import numpy as np
from random import shuffle
# data
import pandas as pd
import gc



# https://stats.wikimedia.org/#/all-projects
# https://meta.wikimedia.org/wiki/List_of_Wikipedias/ca
# https://meta.wikimedia.org/wiki/Research:Metrics#Volume_of_contribution
# https://meta.wikimedia.org/wiki/Research:Wikistats_metrics/Active_editors



# MAIN
def main():




    for languagecode in wikilanguagecodes: # wikilanguagecodes

        create_community_health_metrics_db()
        editor_metrics_dump_iterator(languagecode) # it fills the database cawiki_editors, cawiki_editor_metrics
        print ('dump.\n')
        input('')

        editor_metrics_db_iterator(languagecode) # it fills the database cawiki_editor_metrics
        print ('database.\n')
        input('')

        community_metrics_db_iterator(languagecode) # it fills the database cawiki_community_metrics

    print ('done')

    ###
    # export_community_health_metrics_csv(languagecode) # it fills the database cawiki_editor_metrics
    # editor_metrics_content_diversity(languagecode)    
    # editor_metrics_multilingual(languagecode)



################################################################

# FUNCTIONS

def create_community_health_metrics_db():

    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()

    
    for languagecode in wikilanguagecodes:


        table_name = languagecode+'wiki_editors'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, bot text, user_flags text, highest_flag text, highest_flag_year_month text, gender text, primarybinary integer, primarylang text, primarybinary_ecount integer, totallangs_ecount integer, numberlangs integer, registration_date, year_month_registration, first_edit_timestamp text, year_month_first_edit text, year_first_edit text, lustrum_first_edit text, survived60d text, last_edit_timestamp text, year_last_edit text, lifetime_days integer, editing_days integer, percent_editing_days real, days_since_last_edit integer, seconds_between_last_two_edits integer, PRIMARY KEY (user_id, user_name))")
        cursor.execute(query)


        table_name = languagecode+'wiki_editor_metrics'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, abs_value real, rel_value real, metric_name text, year_month text, timestamp text, PRIMARY KEY (user_id, metric_name, year_month, timestamp))")
        cursor.execute(query)


        table_name = languagecode+'wiki_community_metrics'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (year_month text, group_name text, g1_variable text, g1_calculation text, g1_value text, g2_variable text, g2_calculation text, g2_value text, g1_count float, g2_count float, PRIMARY KEY (group_name, g1_variable, g1_calculation, g1_value, g2_variable, g2_calculation, g2_value))")
        cursor.execute(query)


        table_name = languagecode+'wiki_article_metrics'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (qitem text, page_id integer, page_title text, abs_value real, rel_value, metric_name text, year_month text, PRIMARY KEY (metric_name, year_month))")
        cursor.execute(query)


        ### ---- ###

        table_name = languagecode+'wiki_editor_content_metrics'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, content_type text, abs_value real, rel_value real, year_month text, PRIMARY KEY (user_name, user_id, content_type))")
        cursor.execute(query)

    conn.commit()




def get_mediawiki_paths(languagecode):

    cym = cycle_year_month
    d_paths = []

    print ('/public/dumps/public/other/mediawiki_history/'+cym)
    if os.path.isdir('/public/dumps/public/other/mediawiki_history/'+cym)==False:
        cym = datetime.datetime.strptime(cym,'%Y-%m')-dateutil.relativedelta.relativedelta(months=1)
        cym = cym.strftime('%Y-%m')
        print ('/public/dumps/public/other/mediawiki_history/'+cym)

    dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.all-time.tsv.bz2'
    if os.path.isfile(dumps_path):
        print ('one all-time file.')
        d_paths.append(dumps_path)

    else:
        print ('multiple files.')
        for year in range (1999, 2025):
            dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'.tsv.bz2'
            if os.path.isfile(dumps_path): 
                d_paths.append(dumps_path)

        if len(d_paths) == 0:
            for year in range(1999, 2025): # months
                for month in range(0, 13):
                    if month > 9:
                        dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'-'+str(month)+'.tsv.bz2'
                    else:
                        dumps_path = '/public/dumps/public/other/mediawiki_history/'+cym+'/'+languagecode+'wiki/'+cym+'.'+languagecode+'wiki.'+str(year)+'-0'+str(month)+'.tsv.bz2'

                    if os.path.isfile(dumps_path) == True:
                        d_paths.append(dumps_path)

    print(len(d_paths))
    print (d_paths)

    return d_paths, cym




def editor_metrics_dump_iterator(languagecode):

    functionstartTime = time.time()
    function_name = 'editor_metrics_dump_iterator '+languagecode
    print (function_name)

    d_paths, cym = get_mediawiki_paths(languagecode)


    if (len(d_paths)==0):
        print ('dump error. this language has no mediawiki_history dump: '+languagecode)
        # wikilanguages_utils.send_email_toolaccount('dump error at script '+script_name, dumps_path)
        # quit()

    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()


    user_id_user_name_dict = {}
    user_id_bot_dict = {}
    user_id_user_groups_dict = {}

    editor_first_edit_timestamp = {}
    editor_registration_date = {}

    editor_last_edit_timestamp = {}
    editor_seconds_since_last_edit = {}

    editor_user_group_dict = {}
    editor_user_group_dict_timestamp = {}

    # for the survival part
    survived_dict = {}
    survival_measures = []
    user_id_edit_count = {}
    editor_user_page_edit_count = {}
    editor_user_page_talk_page_edit_count = {}


    # for the monthly part
    editor_monthly_namespace0_edits = {}
    editor_monthly_namespace1_edits = {}
    editor_monthly_namespace2_edits = {}
    editor_monthly_namespace3_edits = {}
    editor_monthly_namespace4_edits = {}
    editor_monthly_namespace5_edits = {}
    editor_monthly_namespace6_edits = {}
    editor_monthly_namespace7_edits = {}
    editor_monthly_namespace8_edits = {}
    editor_monthly_namespace9_edits = {}
    editor_monthly_namespace10_edits = {}
    editor_monthly_namespace11_edits = {}
    editor_monthly_namespace12_edits = {}
    editor_monthly_namespace13_edits = {}
    editor_monthly_namespace14_edits = {}
    editor_monthly_namespace15_edits = {}
    editor_monthly_user_page_edit_count = {}
    editor_monthly_user_page_talk_page_edit_count = {}
    editor_monthly_edits = {}
    editor_monthly_seconds_between_edits = {}
    editor_monthly_editing_days = {}

   

    last_year_month = 0
    first_date = datetime.datetime.strptime('2001-01-01 01:15:15','%Y-%m-%d %H:%M:%S')

    for dump_path in d_paths:

        print('\n'+dump_path)
        iterTime = time.time()

        dump_in = bz2.open(dump_path, 'r')
        line = 'something'
        line = dump_in.readline()


        while line != '':

            # print ('*')
            # print (line)
            # print (seconds_since_last_edit)
            # print ('*')
            # input('')            

            line = dump_in.readline()
            line = line.rstrip().decode('utf-8')[:-1]
            values = line.split('\t')
            if len(values)==1: continue

            event_entity = values[1]
            event_type = values[2]



            event_user_id = values[5]
            try: int(event_user_id)
            except: 
                continue

            event_user_text = values[7]
            if event_user_text != '': user_id_user_name_dict[event_user_id] = event_user_text
            else: 
                continue


            try:
                editor_last_edit = editor_last_edit_timestamp[event_user_id]
                last_edit_date_dt = datetime.datetime.strptime(editor_last_edit[:len(editor_last_edit)-2],'%Y-%m-%d %H:%M:%S')
                last_edit_year_month_day = datetime.datetime.strptime(last_edit_date_dt.strftime('%Y-%m-%d'),'%Y-%m-%d')
            except:
                last_edit_year_month_day = ''


            event_timestamp = values[3]
            event_timestamp_dt = datetime.datetime.strptime(event_timestamp[:len(event_timestamp)-2],'%Y-%m-%d %H:%M:%S')
            editor_last_edit_timestamp[event_user_id] = event_timestamp


            event_user_groups = values[11]
            if event_user_groups != '':
                user_id_user_groups_dict[event_user_id] = event_user_groups


            # if event_user_id == 213 or event_user_text == 'Stebbiv':
            #     print ('***')
            #     print (event_user_id, event_user_text, event_user_groups)
            #     input('')

            if event_type == 'altergroups':
                user_id = values[36]
                user_group = values[41]
                cur_ug = ''

                if user_group != '' and user_group != None:
                  
                    try:
                        cur_ug = editor_user_group_dict[user_id]


                        if len(cur_ug) < len(user_group):
                            change = user_group.replace(cur_ug,'').strip(',')
                            metric_name = 'granted_flag'
                        else:
                            change = cur_ug.replace(user_group,'').strip(',')
                            metric_name = 'removed_flag' # this is only for the case that one flag is removed by another editor. when an editor removes him/herself the flag, it does not appear here.
                    except:
                        change = user_group
                        metric_name = 'granted_flag'


                    # change (what is new + o -); 
                    # user_group (what is he has after the change); 
                    # cur_ug (what he had right before); 
                    # values[42] (what he'll have in the future and in the end) 

                    # input('')
                    editor_user_group_dict[user_id] = user_group


                    if change != '':

                        # user_text = values[38]
                        # print (user_id, user_text, ' - ', change, ' - ', user_group, ' - ', cur_ug ,' - ', values[42], ' - ', metric_name, event_timestamp)                  
                        # print ('\n',event_type, event_entity, event_user_text, cur_ug, event_user_groups,'\n',line)

        
                        if ',' in change:
                            change_ = change.split(',')
                            event_timestamp2 = event_timestamp[:len(event_timestamp)-2] 
                            editor_user_group_dict_timestamp[user_id,event_timestamp] = [metric_name, change_[0], cur_ug]
                            editor_user_group_dict_timestamp[user_id,event_timestamp2] = [metric_name, change_[1], cur_ug]

                        else:
                            editor_user_group_dict_timestamp[user_id,event_timestamp] = [metric_name, change, cur_ug]

                        # if ',' in change:
                        #     input('')


            event_is_bot_by = values[13]
            if event_is_bot_by != '':
                user_id_bot_dict[event_user_id] = event_is_bot_by
                # print (event_user_text, event_is_bot_by)

            event_user_is_anonymous = values[17]
            if event_user_is_anonymous == True or event_user_id == '': continue

            event_user_registration_date = values[18]
            if event_user_id not in editor_registration_date: 
                if event_user_registration_date != '':
                    editor_registration_date[event_user_id] = event_user_registration_date


            ####### ---------

            # MONTHLY EDITS COUNTER
            try: editor_monthly_edits[event_user_id] = editor_monthly_edits[event_user_id]+1
            except: editor_monthly_edits[event_user_id] = 1


            # MONTHLY NAMESPACES EDIT COUNTER
            page_namespace = values[28]
            if page_namespace == '0':
                try: editor_monthly_namespace0_edits[event_user_id] = editor_monthly_namespace0_edits[event_user_id]+1
                except: editor_monthly_namespace0_edits[event_user_id] = 1
            elif page_namespace == '1':
                try: editor_monthly_namespace1_edits[event_user_id] = editor_monthly_namespace1_edits[event_user_id]+1
                except: editor_monthly_namespace1_edits[event_user_id] = 1
            elif page_namespace == '2':
                try: editor_monthly_namespace2_edits[event_user_id] = editor_monthly_namespace2_edits[event_user_id]+1
                except: editor_monthly_namespace2_edits[event_user_id] = 1
            elif page_namespace == '3':
                try: editor_monthly_namespace3_edits[event_user_id] = editor_monthly_namespace3_edits[event_user_id]+1
                except: editor_monthly_namespace3_edits[event_user_id] = 1
            elif page_namespace == '4':
                try: editor_monthly_namespace4_edits[event_user_id] = editor_monthly_namespace4_edits[event_user_id]+1
                except: editor_monthly_namespace4_edits[event_user_id] = 1
            elif page_namespace == '5':
                try: editor_monthly_namespace5_edits[event_user_id] = editor_monthly_namespace5_edits[event_user_id]+1
                except: editor_monthly_namespace5_edits[event_user_id] = 1
            elif page_namespace == '6':
                try: editor_monthly_namespace6_edits[event_user_id] = editor_monthly_namespace6_edits[event_user_id]+1
                except: editor_monthly_namespace6_edits[event_user_id] = 1
            elif page_namespace == '7':
                try: editor_monthly_namespace7_edits[event_user_id] = editor_monthly_namespace7_edits[event_user_id]+1
                except: editor_monthly_namespace7_edits[event_user_id] = 1
            elif page_namespace == '8':
                try: editor_monthly_namespace8_edits[event_user_id] = editor_monthly_namespace8_edits[event_user_id]+1
                except: editor_monthly_namespace8_edits[event_user_id] = 1
            elif page_namespace == '9':
                try: editor_monthly_namespace9_edits[event_user_id] = editor_monthly_namespace9_edits[event_user_id]+1
                except: editor_monthly_namespace9_edits[event_user_id] = 1
            elif page_namespace == '10':
                try: editor_monthly_namespace10_edits[event_user_id] = editor_monthly_namespace10_edits[event_user_id]+1
                except: editor_monthly_namespace10_edits[event_user_id] = 1
            elif page_namespace == '11':
                try: editor_monthly_namespace11_edits[event_user_id] = editor_monthly_namespace11_edits[event_user_id]+1
                except: editor_monthly_namespace11_edits[event_user_id] = 1
            elif page_namespace == '12':
                try: editor_monthly_namespace12_edits[event_user_id] = editor_monthly_namespace12_edits[event_user_id]+1
                except: editor_monthly_namespace12_edits[event_user_id] = 1
            elif page_namespace == '13':
                try: editor_monthly_namespace13_edits[event_user_id] = editor_monthly_namespace13_edits[event_user_id]+1
                except: editor_monthly_namespace13_edits[event_user_id] = 1
            elif page_namespace == '14':
                try: editor_monthly_namespace14_edits[event_user_id] = editor_monthly_namespace14_edits[event_user_id]+1
                except: editor_monthly_namespace14_edits[event_user_id] = 1
            elif page_namespace == '15':
                try: editor_monthly_namespace15_edits[event_user_id] = editor_monthly_namespace15_edits[event_user_id]+1
                except: editor_monthly_namespace15_edits[event_user_id] = 1


            # MONTHLY USER PAGE/USER PAGE TALK PAGE EDIT COUNTER
            page_title = values[25]
            if event_user_text == page_title and page_namespace == '2':
                try: 
                    editor_monthly_user_page_edit_count[event_user_id] = editor_monthly_user_page_edit_count[event_user_id]+1
                except: 
                    editor_monthly_user_page_edit_count[event_user_id] = 1

            if event_user_text == page_title and page_namespace == '3':
                try:
                    editor_monthly_user_page_talk_page_edit_count[event_user_id] = editor_monthly_user_page_talk_page_edit_count[event_user_id]+1
                except:
                    editor_monthly_user_page_talk_page_edit_count[event_user_id] = 1


            # MONTHLY AVERAGE SECONDS BETWEEN EDITS COUNTER
            seconds_since_last_edit = values[22]
            if seconds_since_last_edit != None and seconds_since_last_edit != '':
                seconds_since_last_edit = int(seconds_since_last_edit)
                editor_seconds_since_last_edit[event_user_id] = seconds_since_last_edit
            if seconds_since_last_edit != None and seconds_since_last_edit != '':
                if event_user_id != '' and event_user_id != 0:
                    try:
                        editor_monthly_seconds_between_edits[event_user_id].append(seconds_since_last_edit)
                    except:
                        editor_monthly_seconds_between_edits[event_user_id] = [seconds_since_last_edit]


            # COUNTING DAYS
            current_year_month_day = datetime.datetime.strptime(event_timestamp_dt.strftime('%Y-%m-%d'),'%Y-%m-%d')
            if current_year_month_day != last_edit_year_month_day:
                try: editor_monthly_editing_days[event_user_id]+=1
                except: editor_monthly_editing_days[event_user_id]=1


            ####### ---------         

            # CHECK MONTH CHANGE AND INSERT MONTHLY EDITS/NAMESPACES EDITS/SECONDS
            current_year_month = datetime.datetime.strptime(event_timestamp_dt.strftime('%Y-%m'),'%Y-%m')
            if last_year_month != current_year_month and last_year_month != 0:
                lym = last_year_month.strftime('%Y-%m')
                print (current_year_month, lym, cym)

                lym_sp = lym.split('-')
                ly = lym_sp[0]
                lm = lym_sp[1]

                lym_days = calendar.monthrange(int(ly),int(lm))[1]

                monthly_edits = []
                monthly_seconds = []
                namespaces = []


                for user_id, edits in editor_monthly_edits.items():
                    monthly_edits.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits', lym, ''))

                for user_id, seconds_list in editor_monthly_seconds_between_edits.items():
                    if seconds_list == None: continue
                    elif len(seconds_list) > 1:
                        average_seconds = np.mean(seconds_list)
                        monthly_seconds.append((user_id, user_id_user_name_dict[user_id], average_seconds, None, 'monthly_average_seconds_between_edits', lym, ''))

                for user_id, edits in editor_monthly_namespace0_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns0_main', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace1_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns1_talk', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace2_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns2_user', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace3_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns3_user_talk', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace4_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns4_project', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace5_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns5_project_talk', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace6_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns6_file', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace7_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns7_file_talk', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace8_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns8_mediawiki', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace9_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns9_mediawiki_talk', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace10_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns10_template', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace11_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns11_template_talk', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace12_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns12_help', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace13_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns13_help_talk', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace14_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns14_category', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace15_edits.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_ns15_category_talk', lym, ''))
                    except: pass


                for user_id, edits in editor_monthly_user_page_edit_count.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_user_page_edit_count', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_user_page_talk_page_edit_count.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_user_page_talk_page_edit_count', lym, ''))
                    except: pass

                for user_id, days in editor_monthly_editing_days.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], days, 100*(days/lym_days), 'monthly_editing_days', lym, ''))
                    except: pass

                for key, data in editor_user_group_dict_timestamp.items():
                    user_id = key[0]
                    timestamp = key[1]

                    metric_name = data[0]
                    flags = data[1]

                    old_flags = data[2]


                    try:
                        if metric_name == 'removed_flag':
                            namespaces.append((user_id, user_id_user_name_dict[user_id], old_flags, None, metric_name, lym, timestamp))
                            # print ((user_id, user_id_user_name_dict[user_id], old_flags, None, metric_name, lym, timestamp))

                        else:
                            namespaces.append((user_id, user_id_user_name_dict[user_id], flags, None, metric_name, lym, timestamp))
                            # print ((user_id, user_id_user_name_dict[user_id], flags, None, metric_name, lym, timestamp))



                    except: pass                    


                query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
                cursor.executemany(query,monthly_edits)
                cursor.executemany(query,namespaces)
                cursor.executemany(query,monthly_seconds)
                conn.commit()

                monthly_edits = []
                monthly_seconds = []
                namespaces = []

                editor_monthly_namespace0_edits = {}
                editor_monthly_namespace1_edits = {}
                editor_monthly_namespace2_edits = {}
                editor_monthly_namespace3_edits = {}
                editor_monthly_namespace4_edits = {}
                editor_monthly_namespace5_edits = {}
                editor_monthly_namespace6_edits = {}
                editor_monthly_namespace7_edits = {}
                editor_monthly_namespace8_edits = {}
                editor_monthly_namespace9_edits = {}
                editor_monthly_namespace10_edits = {}
                editor_monthly_namespace11_edits = {}
                editor_monthly_namespace12_edits = {}
                editor_monthly_namespace13_edits = {}
                editor_monthly_namespace14_edits = {}
                editor_monthly_namespace15_edits = {}
                editor_monthly_edits = {}
                editor_monthly_seconds_between_edits = {}
                editor_monthly_user_page_edit_count = {}
                editor_monthly_user_page_talk_page_edit_count = {}
                editor_monthly_editing_days = {}
                editor_user_group_dict_timestamp = {}

        
            last_year_month = current_year_month

            ####### ---------

            # SURVIVAL MEASURES
            event_user_first_edit_timestamp = values[20]
            if event_user_id not in editor_first_edit_timestamp:
                editor_first_edit_timestamp[event_user_id] = event_user_first_edit_timestamp

            if event_user_first_edit_timestamp == '' or event_user_first_edit_timestamp == None:
                event_user_first_edit_timestamp = editor_first_edit_timestamp[event_user_id]

            if event_user_first_edit_timestamp != '' and event_user_id not in survived_dict:
                event_user_first_edit_timestamp_dt = datetime.datetime.strptime(event_user_first_edit_timestamp[:len(event_user_first_edit_timestamp)-2],'%Y-%m-%d %H:%M:%S')


                # thresholds
                first_edit_timestamp_1day_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(days=1))
                first_edit_timestamp_7days_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(days=7))
                first_edit_timestamp_1months_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(months=1))
                first_edit_timestamp_2months_dt = (event_user_first_edit_timestamp_dt + relativedelta.relativedelta(months=2))

                try: ec = user_id_edit_count[event_user_id]
                except: ec = 1


                # at 1 day
                if event_timestamp_dt >= first_edit_timestamp_1day_dt:

                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_24h', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_edit_count:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_edit_count[event_user_id], None, 'user_page_edit_count_24h', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_talk_page_edit_count:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_talk_page_edit_count[event_user_id], None, 'user_page_talk_page_edit_count_24h', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))


                # at 7 days
                if event_timestamp_dt >= first_edit_timestamp_7days_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_7d', first_edit_timestamp_7days_dt.strftime('%Y-%m'),first_edit_timestamp_7days_dt.strftime('%Y-%m-%d %H:%M:%S')))

                # at 1 month
                if event_timestamp_dt >= first_edit_timestamp_1months_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_30d', first_edit_timestamp_1months_dt.strftime('%Y-%m'),first_edit_timestamp_1months_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_edit_count:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_edit_count[event_user_id], None, 'user_page_edit_count_1month', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                    if event_user_id in editor_user_page_talk_page_edit_count:
                        survival_measures.append((event_user_id, event_user_text, editor_user_page_talk_page_edit_count[event_user_id], None, 'user_page_talk_page_edit_count_1month', first_edit_timestamp_1day_dt.strftime('%Y-%m'),first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))


                # at 2 months
                if event_timestamp_dt >= first_edit_timestamp_2months_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_60d', first_edit_timestamp_2months_dt.strftime('%Y-%m'),first_edit_timestamp_2months_dt.strftime('%Y-%m-%d %H:%M:%S')))
                    survived_dict[event_user_id]=event_user_text

                    try: del user_id_edit_count[event_user_id]
                    except: pass

                    try: del editor_user_page_talk_page_edit_count[event_user_id]
                    except: pass

                    try: del editor_user_page_edit_count[event_user_id]
                    except: pass


            # USER PAGE EDIT COUNT, ADD ONE MORE EDIT.
            if event_user_id not in survived_dict:

                if event_user_text == page_title and page_namespace == '2':
                    try: 
                        editor_user_page_edit_count[event_user_id] = editor_user_page_edit_count[event_user_id]+1
                    except: 
                        editor_user_page_edit_count[event_user_id] = 1

                if event_user_text == page_title and page_namespace == '3':
                    try:
                        editor_user_page_talk_page_edit_count[event_user_id] = editor_user_page_talk_page_edit_count[event_user_id]+1
                    except:
                        editor_user_page_talk_page_edit_count[event_user_id] = 1

                # EDIT COUNT, ADD ONE MORE EDIT.
                event_user_revision_count = values[21]
                if event_user_revision_count != '':
                    user_id_edit_count[event_user_id] = event_user_revision_count
                elif event_user_id in user_id_edit_count:
                    user_id_edit_count[event_user_id] = int(user_id_edit_count[event_user_id]) + 1
                else:
                    user_id_edit_count[event_user_id] = 1

            ####### ---------




        # SURVIVAL MEASURES INSERT
        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
        cursor.executemany(query,survival_measures)
        conn.commit()
        survival_measures = []



        # MONTHLY EDITS/SECONDS INSERT (LAST ROUND)
        lym = last_year_month.strftime('%Y-%m')

        if lym != cym:

            monthly_edits = []
            for event_user_id, edits in editor_monthly_edits.items():
                monthly_edits.append((event_user_id, user_id_user_name_dict[event_user_id], edits, None, 'monthly_edits', lym, ''))

            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
            cursor.executemany(query,monthly_edits)
            conn.commit()
            editor_monthly_edits = {}
            monthly_edits = []


            monthly_seconds = []
            for event_user_id, seconds_list in editor_monthly_seconds_between_edits.items():
                if seconds_list == None: continue
                elif len(seconds_list) > 1:
                    average_seconds = np.mean(seconds_list)
                    monthly_seconds.append((event_user_id, user_id_user_name_dict[event_user_id], average_seconds, None, 'monthly_average_seconds_between_edits', lym, ''))

            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
            cursor.executemany(query,monthly_seconds)
            conn.commit()
            editor_monthly_seconds_between_edits = {}
            monthly_seconds = []




        # USER CHARACTERISTICS INSERT
        user_characteristics1 = []
        user_characteristics2 = []
        for user_id, user_name in user_id_user_name_dict.items():
            
            try: user_flags = user_id_user_groups_dict[user_id]
            except: user_flags = ''

            try: bot = user_id_bot_dict[user_id]
            except: bot = 'editor'

            if user_id in survived_dict: survived60d = '1'
            else: survived60d = '0'


            try: registration_date = editor_registration_date[user_id]
            except: registration_date = ''
            
            if registration_date == '': # THIS IS SOMETHING WE "ASSUME" BECAUSE THERE ARE MANY ACCOUNTS WITHOUT A REGISTRATION DATE.
                try: registration_date = editor_first_edit_timestamp[user_id]
                except: registration_date = ''

            if registration_date != '': year_month_registration = datetime.datetime.strptime(registration_date[:len(registration_date)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
            else: year_month_registration = ''

            try: fe = editor_first_edit_timestamp[user_id]
            except: fe = ''

            try: 
                le = editor_last_edit_timestamp[user_id]
                year_last_edit = datetime.datetime.strptime(le[:len(le)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y')

            except: 
                le = ''
                year_last_edit


            if fe != '':  
                year_month = datetime.datetime.strptime(fe[:len(fe)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
                year_first_edit = datetime.datetime.strptime(fe[:len(fe)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y')

                if int(year_first_edit) >= 2001 < 2006: lustrum_first_edit = '2001-2005'
                if int(year_first_edit) >= 2006 < 2011: lustrum_first_edit = '2006-2010'
                if int(year_first_edit) >= 2011 < 2016: lustrum_first_edit = '2011-2015'
                if int(year_first_edit) >= 2016 < 2021: lustrum_first_edit = '2016-2020'
                if int(year_first_edit) >= 2020 < 2026: lustrum_first_edit = '2021-2025'

                fe_d = datetime.datetime.strptime(fe[:len(fe)-2],'%Y-%m-%d %H:%M:%S')
            else:
                year_month = ''
                year_first_edit = ''
                lustrum_first_edit = ''
                fe_d = ''


            if le != '':
                le_d = datetime.datetime.strptime(le[:len(le)-2],'%Y-%m-%d %H:%M:%S')
                days_since_last_edit = (event_timestamp_dt - le_d).days
            else:
                le_d = ''
                days_since_last_edit = ''


            if fe != '' and le != '': lifetime_days =  (le_d - fe_d).days
            else: lifetime_days = 0
        
            try: se = editor_seconds_since_last_edit[user_id]
            except: se = ''

            user_characteristics1.append((user_id, user_name, registration_date, year_month_registration,  fe, year_month, year_first_edit, lustrum_first_edit, survived60d))

            user_characteristics2.append((bot, user_flags, le, year_last_edit, lifetime_days, days_since_last_edit, se, user_id, user_name))


        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editors (user_id, user_name, registration_date, year_month_registration, first_edit_timestamp, year_month_first_edit, year_first_edit, lustrum_first_edit, survived60d) VALUES (?,?,?,?,?,?,?,?,?);'
        cursor.executemany(query,user_characteristics1)

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editors (bot, user_flags, last_edit_timestamp, year_last_edit, lifetime_days, days_since_last_edit, seconds_between_last_two_edits, user_id, user_name) VALUES (?,?,?,?,?,?,?,?,?);'
        cursor.executemany(query,user_characteristics2)

        query = 'UPDATE '+languagecode+'wiki_editors SET bot = ?, user_flags = ?, last_edit_timestamp = ?, year_last_edit = ?, lifetime_days = ?, days_since_last_edit = ?, seconds_between_last_two_edits = ? WHERE user_id = ? AND user_name = ?;'
        cursor.executemany(query,user_characteristics2)
        conn.commit()

        user_characteristics1 = []
        user_characteristics2 = []

        # insert or ignore + update
        user_id_bot_dict = {}
        user_id_user_groups_dict = {}
        editor_last_edit_timestamp = {}
        editor_seconds_since_last_edit = {}

        # insert or ignore
        editor_first_edit_timestamp = {}
        editor_registration_date = {}



        # END OF THE DUMP!!!!
        print ('end of the dump.')
        print ('*')
        print (str(datetime.timedelta(seconds=time.time() - iterTime)))








    # AGGREGATED METRICS (EDIT COUNTS)
    monthly_aggregated_metrics = {'monthly_edits':'edit_count', 'monthly_user_page_edit_count': 'edit_count_editor_user_page', 'monthly_user_page_talk_page_edit_count': 'edit_count_editor_user_page_talk_page', 'monthly_edits_ns0_main':'edit_count_ns0_main', 'monthly_edits_ns1_talk':'edit_count_ns1_talk', 'monthly_edits_ns2_user':'edit_count_ns2_user', 'monthly_edits_ns3_user_talk': 'edit_count_ns3_user_talk', 'monthly_edits_ns4_project':'edit_count_ns4_project', 'monthly_edits_ns5_project_talk': 'edit_count_ns5_project_talk', 'monthly_edits_ns6_file': 'edit_count_edits_ns6_file', 'monthly_edits_ns7_file_talk':'edit_count_ns7_file_talk', 'monthly_edits_ns8_mediawiki': 'edit_count_ns8_mediawiki', 'monthly_edits_ns9_mediawiki_talk': 'edit_count_ns9_mediawiki_talk', 'monthly_edits_ns10_template':'edit_count_ns10_template', 'monthly_edits_ns11_template_talk':'edit_count_ns11_template_talk', 'monthly_edits_ns12_help':'edit_count_ns12_help','monthly_edits_ns13_help_talk':'edit_count_ns13_help_talk','monthly_edits_ns14_category':'edit_count_ns14_category','monthly_edits_ns15_category_talk':'edit_count_ns15_category_talk'}


    conn2 = sqlite3.connect(databases_path + community_health_metrics_db); cursor2 = conn2.cursor()
    for monthly_metric_name, metric_name in monthly_aggregated_metrics.items():
        edit_counts = []
        query = 'SELECT user_id, user_name, SUM(abs_value) FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "'+monthly_metric_name+'" GROUP BY 2;'
        for row in cursor.execute(query):
            edit_counts.append((row[0],row[1],row[2],metric_name,lym))

            if metric_name == 'edit_count':
                ec = row[2]
                bin_v = ''
                if ec > 1 and ec <= 100: bin_v = '1-100'
                if ec > 100 and ec <= 500: bin_v = '101-500'
                if ec > 500 and ec <= 1000: bin_v = '501-1000'
                if ec > 1000 and ec <= 5000: bin_v = '1001-5000'
                if ec > 5000 and ec <= 10000: bin_v = '5001-10000'
                if ec > 10000 and ec <= 50000: bin_v = '10001-50000'
                if ec > 50000 and ec <= 100000: bin_v = '50001-100000'
                if ec > 100000 and ec <= 500000: bin_v = '100001-500000'
                if ec > 500000 and ec <= 1000000: bin_v = '500001-1000000'
                if ec > 1000000: bin_v = '1000001+'
                if bin_v != '':
                    edit_counts.append((row[0],row[1],bin_v,'edit_count_bin',lym))

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, metric_name, year_month) VALUES (?,?,?,?,?);';
        cursor2.executemany(query,edit_counts)
        conn2.commit()


    query = 'SELECT user_id, user_name, AVG(abs_value) FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" GROUP BY 2;'
    for row in cursor.execute(query):
        edit_counts.append((row[0],row[1],row[2],'monthly_edit_count_bin',lym))
            ec = row[2]
            bin_v = ''
            if ec > 1 and ec <= 5: bin_v = '1-5'
            if ec > 5 and ec <= 10: bin_v = '6-10'
            if ec > 10 and ec <= 100: bin_v = '11-100'
            if ec > 100 and ec <= 500: bin_v = '101-500'
            if ec > 500 and ec <= 1000: bin_v = '501-1000'
            if ec > 1000 and ec <= 5000: bin_v = '1001-5000'
            if ec > 5000: bin_v = '5001+'
            if bin_v != '':
                edit_counts.append((row[0],row[1],bin_v,'monthly_edit_count_bin',lym))
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, metric_name, year_month) VALUES (?,?,?,?,?);';
        cursor2.executemany(query,edit_counts)
        conn2.commit()




    # FLAGS UPDATE
    # Getting the highest flag
    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()
    query = 'SELECT user_flags, count(user_id) FROM '+languagecode+'wiki_editors WHERE user_flags != "" GROUP BY 1;'
    flags_count_dict = {}
    for row in cursor.execute(query):
        flags = row[0]
        count = row[1]

        if ',' in flags: 
            fs = flags.split(',')
            for x in fs:
                try:
                    flags_count_dict[x]+=count
                except:
                    flags_count_dict[x]=1
        else:
            try:
                flags_count_dict[flags]+=count
            except:
                flags_count_dict[flags]=1

    print ('Number of editors for each flag')
    print (flags_count_dict)
    # print ('in')
    # input('')


    flag_ranks = {
    'confirmed':1,'ipblock-exempt':1,
    'filemover':2,'accountcreator':2,'autopatrolled':2,'reviewer':2,'autoreviewer':2,'rollbacker':2,'abusefilter':2,'abusefilter-ehlper':2,'interface-admin':2,'eventcoordinator':2,'extendedconfirmed':2,'extendedmover':2, 'filemover':2, 'massmessage-sender':2, 'patroller':2, 'researcher':2, 'templateeditor':2,
    'sysop':3,'bureaucrat':3.5,
    'checkuser':4,'oversight':4.5,
    'steward':5.5, 'import':5,
    'founder':6
    }


    query = 'SELECT user_id, user_flags, user_name FROM '+languagecode+'wiki_editors WHERE user_flags != "";'
    params = []
    user_id_flag = {}
    for row in cursor.execute(query):
        user_id = row[0]
        user_flags = row[1]
        user_name = row[2]

        highest_rank = {}
        highest_count = {}


        if ',' in user_flags:
            uf = user_flags.split(',')

            for x in uf:
                if x in flag_ranks and 'bot' not in x:
                    val = flag_ranks[x]
                    highest_rank[x] = val


            if len(highest_rank) > 1:
                maxval = max(highest_rank.values())
                highest_rank = {key:val for key, val in highest_rank.items() if val == maxval} # we are choosing the flag of highest rank.


                if len(highest_rank)>1:
                    for x in highest_rank.keys():
                        val = flags_count_dict[x]
                        highest_count[x] = val
        
                    maxval = max(highest_count.values())
                    highest_count = {key:val for key, val in highest_count.items() if val == maxval} # we are choosing the flag that exists more in the community.

                    f = list(highest_count.keys())[0]
                    params.append((f, user_id, user_name))
                    user_id_flag[user_id]=f
                else:
                    f = list(highest_rank.keys())[0]
                    params.append((f, user_id, user_name))
                    user_id_flag[user_id]=f

        else:
            if user_flags in flag_ranks and 'bot' not in user_flags:
                params.append((user_flags, user_id, user_name))
                user_id_flag[user_id]=user_flags

    query = 'UPDATE '+languagecode+'wiki_editors SET highest_flag = ? WHERE user_id = ? AND user_name = ?;'
    cursor.executemany(query,params)
    conn.commit()
    print ('Updated the editors table with highest flag')


    # let's update the highest_flag_year_month
    query = 'SELECT year_month, user_id, user_name, abs_value FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "granted_flag";'
    params2 = []

    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()

    for row in cursor.execute(query):
        year_month=row[0]
        user_id=row[1]
        user_name=row[2]
        flag = row[3]

        try:
            ex_flag = user_id_flag[user_id]
        except:
            continue

        # print ((ex_flag, flag,year_month,user_id,user_name))
        if ex_flag in flag:
            # print ((ex_flag, flag,year_month,user_id,user_name))
            params2.append((year_month,user_id,user_name))


    # print (params2)
    query = 'UPDATE '+languagecode+'wiki_editors SET highest_flag_year_month = ? WHERE user_id = ? AND user_name = ?;'
    cursor.executemany(query,params2)
    conn.commit()

    print ('Updated the editors table with the year month they obtained the highest flag.')
    # print(list(highest_flag.values()).count('bureaucrat'))


    # If an editor has been granted the 'bot' flag, even if it has been taken away, it must be a flag.
    query = 'SELECT user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "granted_flag" AND abs_value LIKE "%bot";'
    params = []
    for row in cursor.execute(query):
        username = row[1]

        if 'bot' in username:
            bottype = 'name,group'
        else:
            bottype = 'group'
        params.append((bottype,row[0],username))

    query = 'UPDATE '+languagecode+'wiki_editors SET bot = ? WHERE user_id = ? AND user_name = ?;'
    cursor.executemany(query,params)
    conn.commit()

    print ('Updated the table with the bots from flag.')



    def gender(languagecode):

        functionstartTime = time.time()
        function_name = 'gender '+languagecode
        print (function_name)

        conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()


        gender_params = []
        query = 'SELECT up_value, user_name, up_user FROM user INNER JOIN user_properties ON user_id = up_user WHERE up_property = "gender";'
        mysql_con_read = wikilanguages_utils.establish_mysql_connection_read(languagecode); mysql_cur_read = mysql_con_read.cursor()
        mysql_cur_read.execute(query)
        rows = mysql_cur_read.fetchall()
        for row in rows:
            gender_params.append((row[0], row[1], row[2]))
            if len(gender_params) % 10000 == 0:
                query = 'UPDATE '+languagecode+'wiki_editors SET gender = ? WHERE user_id = ? AND user_name = ?;'
                cursor.executemany(query,user_characteristics2)
                conn.commit()
                gender_params = []


        duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
        print(languagecode+' '+ function_name+' '+ duration)

#    gender(languagecode)

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print(languagecode+' '+ function_name+' '+ duration)






def editor_metrics_db_iterator(languagecode):

    functionstartTime = time.time()
    function_name = 'editor_metrics_db_iterator '+languagecode
    print (function_name)

    d_paths, cym = get_mediawiki_paths(languagecode)
    cycle_year_month = cym
    print (cycle_year_month)
    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()

    # MONTHLY EDITS LOOP
    query = 'SELECT abs_value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" ORDER BY user_name, year_month'
    # AND user_name in ("Toniher","Marcmiquel","Barcelona","TaronjaSatsuma","Kippelboy")

    # print (query)
    user_count = 0
    old_user_id = ''
    expected_year_month_dt = ''

    parameters = []
    active_months = 0
    active_months_row = 0
    total_months = 0
    max_active_months_row = 0

    inactivity_periods = 0
    inactive_months = 0
    max_inactive_months_row = 0

    editors_edits_baseline_parameters = []

    total_edits = []

    for row in cursor.execute(query):
        edits=row[0]
        current_year_month = row[1]
        cur_user_id = row[2]
        cur_user_name = row[3]

        if cur_user_id != old_user_id and old_user_id != '':
            user_count += 1

            cycle_year_month_dt = datetime.datetime.strptime(cycle_year_month,'%Y-%m')

            months_since_last_edit = (cycle_year_month_dt.year - current_year_month_dt.year) * 12 + cycle_year_month_dt.month - current_year_month_dt.month

            if months_since_last_edit < 0: months_since_last_edit = 0

            if months_since_last_edit > 0:
                parameters.append((old_user_id, old_user_name, months_since_last_edit, None, 'months_since_last_edit', old_year_month,''))

            if months_since_last_edit > max_inactive_months_row:
                parameters.append((old_user_id, old_user_name, months_since_last_edit, None, 'max_inactive_months_row', old_year_month,''))

                parameters.append((old_user_id, old_user_name, 1, None, 'over_past_max_inactive_months_row', cycle_year_month,''))
            else:
                parameters.append((old_user_id, old_user_name, max_inactive_months_row, None, 'max_inactive_months_row', old_year_month,''))                

            parameters.append((old_user_id, old_user_name, inactivity_periods, None, 'inactivity_periods', old_year_month,''))
            parameters.append((old_user_id, old_user_name, active_months, None, 'active_months', old_year_month,''))
            parameters.append((old_user_id, old_user_name, max_active_months_row, None, 'max_active_months_row', old_year_month,''))
            parameters.append((old_user_id, old_user_name, total_months, None, 'total_months', old_year_month,''))

            active_months = 0
            total_months = 0
            active_months_row = 0
            max_active_months_row = 0
            inactivity_periods = 0
            inactive_months = 0
            max_inactive_months_row = 0

            total_edits = []

            if user_count % 10000 == 0:

                query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
                cursor.executemany(query,parameters)
                conn.commit()
                parameters = []


                # edits compared to baseline
                query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
                cursor.executemany(query,editors_edits_baseline_parameters)
                conn.commit()
                editors_edits_baseline_parameters = []

        current_year_month_dt = datetime.datetime.strptime(current_year_month,'%Y-%m')


        # here there is a change of month
        if expected_year_month_dt != current_year_month_dt and expected_year_month_dt != '' and old_user_id == cur_user_id:

            inactivity_periods += 1
            while expected_year_month_dt < current_year_month_dt:
                # print (expected_year_month_dt, current_year_month_dt)
                inactive_months = inactive_months + 1

                expected_year_month_dt = (expected_year_month_dt + relativedelta.relativedelta(months=1))
                total_months = total_months + 1

            if inactive_months > max_inactive_months_row:
                max_inactive_months_row = inactive_months

            if active_months_row > max_active_months_row:
                max_active_months_row = active_months_row

            parameters.append((cur_user_id, cur_user_name, inactive_months, None, 'inactive_months_row', current_year_month,''))          
            active_months_row = 1
            inactive_months = 0
        else:
            active_months_row = active_months_row + 1

            if active_months_row > 1:
                parameters.append((cur_user_id, cur_user_name, active_months_row, None, 'active_months_row', current_year_month,''))

            if active_months_row > max_active_months_row:
                max_active_months_row = active_months_row

            if inactive_months == 0 and total_months == 0:
                parameters.append((cur_user_id, cur_user_name, -1, None, 'inactive_months_row', current_year_month,''))
            else:
                parameters.append((cur_user_id, cur_user_name, inactive_months, None, 'inactive_months_row', current_year_month,''))


        if total_edits != []:
            median_total_edits = np.median(total_edits)
            editors_edits_baseline_parameters.append((cur_user_id, cur_user_name, (100*edits/median_total_edits - 100), None, "monthly_edits_to_baseline", current_year_month, None))

        total_edits.append(edits)

        total_months = total_months + 1
        active_months = active_months + 1

        old_year_month = current_year_month
        expected_year_month_dt = (datetime.datetime.strptime(old_year_month,'%Y-%m') + relativedelta.relativedelta(months=1))

        old_user_id = cur_user_id
        old_user_name = cur_user_name
        # print ('# update: ',old_user_id, old_user_name, active_months, max_active_months_row, max_inactive_months_row, total_months)
        # input('')


    cycle_year_month_dt = datetime.datetime.strptime(cycle_year_month,'%Y-%m')
    months_since_last_edit = (cycle_year_month_dt.year - current_year_month_dt.year) * 12 + cycle_year_month_dt.month - current_year_month_dt.month
    if months_since_last_edit < 0: months_since_last_edit = 0

    if months_since_last_edit > 0:
        parameters.append((old_user_id, old_user_name, months_since_last_edit, None, 'months_since_last_edit', old_year_month,''))

    if months_since_last_edit > max_inactive_months_row:
        parameters.append((old_user_id, old_user_name, months_since_last_edit, None, 'max_inactive_months_row', old_year_month,''))
        parameters.append((old_user_id, old_user_name, 1, None, 'over_past_max_inactive_months_row', cycle_year_month,''))
    else:
        parameters.append((old_user_id, old_user_name, max_inactive_months_row, None, 'max_inactive_months_row', old_year_month,''))                

    parameters.append((old_user_id, old_user_name, inactivity_periods, None, 'inactivity_periods', old_year_month,''))
    parameters.append((old_user_id, old_user_name, active_months, None, 'active_months', old_year_month,''))
    parameters.append((old_user_id, old_user_name, max_active_months_row, None, 'max_active_months_row', old_year_month,''))
    parameters.append((old_user_id, old_user_name, total_months, None, 'total_months', old_year_month,''))


    # INSERT THE LAST EDITORS
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'

    cursor.executemany(query,parameters)
    conn.commit()
    parameters = []


  # edits compared to baseline
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
    cursor.executemany(query,editors_edits_baseline_parameters)
    conn.commit()
    editors_edits_baseline_parameters = []

    print ('done with the monthly edits.')





    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + community_health_metrics_db); cursor2 = conn2.cursor()

    # MONTHLY EDITING DAYS LOOP
    query = 'SELECT abs_value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_editing_days" ORDER BY user_id, year_month'

    # print (query)
    user_count = 0
    old_user_id = ''
    expected_year_month_dt = ''

    editing_days = []
    sum_editing_days = 0
    # os.remove(databases_path +'temporary_editors.txt')
    # os.remove(databases_path +'temporary_editor_metrics.txt')

    edfile = open(databases_path+'temporary_editors.txt', "w")
    edfile2 = open(databases_path+'temporary_editor_metrics.txt', "w")

    for row in cursor.execute(query):
        monthly_editing_days=row[0]
        current_year_month = row[1]
        cur_user_id = row[2]
        cur_user_name = row[3]

        # print (row)
        if cur_user_id != old_user_id and old_user_id != '':
            user_count += 1
            sum_editing_days = int(sum(editing_days))
            edfile.write(str(sum_editing_days)+'\t'+str(old_user_id)+'\t'+old_user_name+'\n')

            if editing_days != []:
                median_editing_days = np.median(editing_days)
                if median_editing_days == 0:
                    value = 0
                else:
                    value = (100*monthly_editing_days/median_editing_days - 100)

                    edfile2.write(str(old_user_id)+'\t'+old_user_name+','+str(value)+'\t'+" "+'\t'+"monthly_editing_days_to_baseline"+'\t'+current_year_month+'\n')

            sum_editing_days = 0
            editing_days = []


        current_year_month_dt = datetime.datetime.strptime(current_year_month,'%Y-%m')
        if expected_year_month_dt != current_year_month_dt and expected_year_month_dt != '' and old_user_id == cur_user_id:
            while expected_year_month_dt < current_year_month_dt:
                editing_days.append(0)
                expected_year_month_dt = (expected_year_month_dt + relativedelta.relativedelta(months=1))

        editing_days.append(monthly_editing_days)

        old_year_month = current_year_month
        expected_year_month_dt = (datetime.datetime.strptime(old_year_month,'%Y-%m') + relativedelta.relativedelta(months=1))

        old_user_id = cur_user_id
        old_user_name = cur_user_name

    # print ('out of the loop')
    # print (user_count)

    # last row percent baseline
    if editing_days != []:
        median_editing_days = np.median(editing_days)
        if median_editing_days == 0:
            value = 0
        else:
            value = (100*monthly_editing_days/median_editing_days - 100)
            edfile2.write(str(old_user_id)+'\t'+old_user_name+'\t'+str(value)+'\t'+" "+'\t'+"monthly_editing_days_to_baseline"+'\t'+current_year_month+'\n')

    sum_editing_days = sum(editing_days)
    edfile.write(str(sum_editing_days)+','+str(old_user_id)+','+old_user_name+'\n')



    # BASELINE MEASURES
    edfile = open(databases_path+'temporary_editor_metrics.txt', "r")
    editors_metrics_parameters = []
    while True:
        user_count+=1
        line = edfile.readline()
        char = line.strip().split('\t')
        try:
            metric_name = char[4]
            if metric_name != '': editors_metrics_parameters.append((char[0],char[1],char[2],char[3],metric_name,char[5]))
        except:
            pass
        if user_count % 10000 == 0:
            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month) VALUES (?,?,?,?,?,?);'
            cursor2.executemany(query,editors_metrics_parameters)
            conn2.commit()
            editors_metrics_parameters = []
        if not line: break
    os.remove(databases_path +'temporary_editor_metrics.txt')


    # EDITING DAYS
    # sum
    edfile = open(databases_path+'temporary_editors.txt', "r")
    editors_characteristics_parameters = []
    while True:
        user_count+=1
        line = edfile.readline()
        char = line.strip().split('\t')
        try:
            editors_characteristics_parameters.append((char[0],char[1],char[2]))
        except:
            pass
        if user_count % 10000 == 0:
            query = 'UPDATE '+languagecode+'wiki_editors SET editing_days = ? WHERE user_id = ? AND user_name = ?;'
            cursor2.executemany(query,editors_characteristics_parameters)
            conn2.commit()
            editors_characteristics_parameters = []
        if not line: break
    os.remove(databases_path +'temporary_editors.txt')

    # percent
    query = 'UPDATE '+languagecode+'wiki_editors SET percent_editing_days = (100*editing_days/lifetime_days);'
    cursor.execute(query)
    conn.commit()

    print ('done with the monthly editing days.')
    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))



    #### --------- --------- --------- --------- --------- --------- --------- --------- ---------

    # OVER PAST MAX INACTIVE MONTHS ROW
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) SELECT i1.user_id, i1.user_name, (i1.abs_value - i2.abs_value), i1.rel_value, "over_past_max_inactive_months_row", i2.year_month, i2.timestamp FROM '+languagecode+'wiki_editor_metrics i1 INNER JOIN '+languagecode+'wiki_editor_metrics i2 ON i1.user_id = i2.user_id WHERE i1.metric_name = "max_inactive_months_row" AND i2.metric_name = "months_since_last_edit";'
    cursor.execute(query)
    conn.commit()


    # OVER EDIT BIN AVERAGE PAST MAX INACTIVE MONTHS ROW
    edit_bin_average_past_max_inactive_months_row = {}
    query = 'SELECT i2.abs_value, AVG(i1.abs_value) FROM '+languagecode+'wiki_editor_metrics i1 INNER JOIN '+languagecode+'wiki_editor_metrics i2 ON i1.user_id = i2.user_id WHERE i1.metric_name = "max_inactive_months_row" AND i2.metric_name = "edit_count_bin" GROUP BY i2.abs_value;';
    for row in cursor.execute(query):
        edit_bin_average_past_max_inactive_months_row[row[0]]=row[1]

    for edit_bin, average in edit_bin_average_past_max_inactive_months_row.items():
        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) SELECT i1.user_id, i1.user_name, (? - i2.abs_value), i1.rel_value, "over_edit_bin_average_past_max_inactive_months_row", i2.year_month, i2.timestamp FROM '+languagecode+'wiki_editor_metrics i1 INNER JOIN '+languagecode+'wiki_editor_metrics i2 ON i1.user_id = i2.user_id WHERE i1.metric_name = "edit_count_bin" AND i1.abs_value = ? AND i2.metric_name = "months_since_last_edit";'
        cursor.execute(query,(average, edit_bin))
        conn.commit()



    # OVER MONTHLY EDIT BIN AVERAGE PAST MAX INACTIVE MONTHS ROW
    edit_bin_average_past_max_inactive_months_row = {}
    query = 'SELECT i2.abs_value, AVG(i1.abs_value) FROM '+languagecode+'wiki_editor_metrics i1 INNER JOIN '+languagecode+'wiki_editor_metrics i2 ON i1.user_id = i2.user_id WHERE i1.metric_name = "max_inactive_months_row" AND i2.metric_name = "monthly_edit_count_bin" GROUP BY i2.abs_value;';
    for row in cursor.execute(query):
        edit_bin_average_past_max_inactive_months_row[row[0]]=row[1]

    for edit_bin, average in edit_bin_average_past_max_inactive_months_row.items():
        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) SELECT i1.user_id, i1.user_name, (? - i2.abs_value), i1.rel_value, "over_monthly_edit_bin_average_past_max_inactive_months_row", i2.year_month, i2.timestamp FROM '+languagecode+'wiki_editor_metrics i1 INNER JOIN '+languagecode+'wiki_editor_metrics i2 ON i1.user_id = i2.user_id WHERE i1.metric_name = "monthly_edit_count_bin" AND i1.abs_value = ? AND i2.metric_name = "months_since_last_edit";'
        cursor.execute(query,(average, edit_bin))
        conn.commit()



    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print(languagecode+' '+ function_name+' '+ duration)




def community_metrics_db_iterator(languagecode):

    functionstartTime = time.time()
    function_name = 'community_metrics_db_iterator '+languagecode
    print (function_name)

    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()

    d_paths, cym = get_mediawiki_paths(languagecode)
    cycle_year_month = cym
    print (cycle_year_month)

    query_cm = 'INSERT OR IGNORE INTO '+languagecode+'wiki_community_metrics (year_month, group_name, g1_variable, g1_calculation, g1_value, g2_variable, g2_calculation, g2_value, g1_count, g2_count) VALUES (?,?,?,?,?,?,?,?,?,?);'



    def participation():

        # participative_editors total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000                    
        parameters = []
        edit_bins_count = {}
        query = 'SELECT count(user_id), abs_value, year_month FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "edit_count_bin" GROUP by abs_value;'
        for row in cursor.execute(query):
            g1_count = row[0]
            g1_value = row[1]
            year_month = row[2]
            edit_bins_count[g1_value] = g1_count

            parameters.append((year_month, 'participative_editors', 'total_edits', 'bin', g1_value, None, None, None, g1_count, None))
        cursor.executemany(query_cm,parameters)
        conn.commit()


        # participative_editors total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000    monthly_edits   threshold   5
        parameters = []
        query = 'SELECT count(e1.user_id), e1.abs_value, e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "edit_count_bin" AND e2.metric_name = "monthly_edits" AND e2.abs_value >= 5 GROUP BY e1.abs_value ORDER BY e1.abs_value DESC;'
        for row in cursor.execute(query):
            g2_count = row[0]
            g1_value = row[1]
            year_month = row[2]
            g1_count = edit_bins_count[g1_value]

            parameters.append((year_month, 'participative_editors', 'total_edits', 'bin', g1_value, 'monthly_edits', 'threshold', 5, g1_count, g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()


        # participative_editors total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000    year_first_edit bin 2001-2021       
        parameters = []
        query = 'SELECT count(e1.user_id), e1.abs_value, e2.year_first_edit, e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "edit_count_bin" GROUP by e1.abs_value, e2.year_first_edit;'
        for row in cursor.execute(query):
            g2_count = row[0]
            g1_value = row[1]
            g2_value = row[2]
            g1_count = edit_bins_count[g1_value]

            year_month = row[3]
            parameters.append((year_month, 'participative_editors', 'total_edits', 'bin', g1_value, 'year_first_edit', 'bin', g2_value, g1_count, g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()


        # participative_editors total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000    lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021    
        parameters = []
        query = 'SELECT count(e1.user_id), e1.abs_value, e2.lustrum_first_edit, e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "edit_count_bin" GROUP by e1.abs_value, e2.lustrum_first_edit;'
        for row in cursor.execute(query):
            g2_count = row[0]
            g1_value = row[1]
            g2_value = row[2]
            g1_count = edit_bins_count[g1_value]
            year_month = row[3]

            parameters.append((year_month, 'participative_editors', 'total_edits', 'bin', g1_value, 'lustrum_first_edit', 'bin', g2_value, g1_count, g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()     
        

        editing_days = {(1,100):'1-100',(101,500):'101-500', (501,1000):'501-1000', (1001,1500):'1001-1500', (1501,2500):'1501-2500', (2501,3500):'2501-3500', (3501,4500):'3501-4500', (4501,5500):'4501-5500', (5501,6500):'5501-6500', (6501,7500):'6501-7500'}

        percent_editing_days = {(0,10):'0-10',(11,20):'11-20',(21,30):'21-30',(31,40):'31-40',(41,50):'41-50',(51,60):'51-60',(61,70):'61-70',(71,80):'71-80',(81,90):'81-90',(91,100):'91-100'}

        active_months = {(0,0):'0', (217, 228): '217-228', (301, 312): '301-312', (277, 288): '277-288', (25, 36): '25-36', (241, 252): '241-252', (109, 120): '109-120', (85, 96): '85-96', (61, 72): '61-72', (205, 216): '205-216', (289, 300): '289-300', (193, 204): '193-204', (73, 84): '73-84', (49, 60): '49-60', (37, 48): '37-48', (265, 276): '265-276', (181, 192): '181-192', (145, 156): '145-156', (13, 24): '13-24', (253, 264): '253-264', (133, 144): '133-144', (1, 12): '1-12', (121, 132): '121-132', (169, 180): '169-180', (157, 168): '157-168', (229, 240): '229-240', (97, 108): '97-108'}

        bin_dicts = {'editing_days':editing_days, 'percent_editing_days':percent_editing_days, 'active_months':active_months}

        # participative_editors   total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000    editing_days    bin 0_100, 100_500, 500_1000, 1000-1500, 1500-2500, 2500-3500, 3500-5000, 5000-6500, 6500-7500…     
        # participative_editors   total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000    percent_editing_days    bin 1-10 to 100     
        # participative_editors   total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000    active_months   bin 1-10, 10-20, 30-40,... to 150       
        parameters = []
        for variable_name, bin_dict in bin_dicts.items():
            for interval, label in bin_dict.items():

                query = 'SELECT count(e1.user_id), e1.abs_value, e1.year_month, "'+label+'" FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "edit_count_bin" AND e2.'+variable_name+' BETWEEN '+str(interval[0])+' AND '+str(interval[1])+' GROUP by e1.abs_value;'

                g2_count = row[0]
                g1_value = row[1]
                g2_value = label
                year_month = row[2]

                g1_count = edit_bins_count[g1_value]
                parameters.append((year_month, 'participative_editors', 'total_edits', 'bin', g1_value, variable_name, 'bin', g2_value, g1_count, g2_count))

                cursor.executemany(query_cm,parameters)
                conn.commit()


        # participative_editors total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000    flag    name    sysop, autopatrolled, bureaucrat, etc.
        parameters = []
        query = 'SELECT count(e1.user_id), e1.abs_value, e2.highest_flag, e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "edit_count_bin" GROUP by e1.abs_value, e2.highest_flag;'
        for row in cursor.execute(query):
            g2_count = row[0]
            g1_value = row[1]
            g2_value = row[2]
            year_month = row[3]

            g1_count = edit_bins_count[g1_value]
            parameters.append((year_month, 'participative_editors', 'total_edits', 'bin', g1_value, 'highest_flag', 'name', g2_value, g1_count, g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()

        print ('participation')




    def flags():

        # flags   granted_flag    name    sysop, autopatrolled, bureaucrat, etc.                  
        # flags   removed_flag    name    sysop, autopatrolled, bureaucrat, etc.                  
        for variablef in ['granted_flag','removed_flag']:
            parameters = []
            year_month = cycle_year_month
            query = 'SELECT count(user_id), abs_value, year_month FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "'+variablef+'" AND abs_value != "bot" GROUP BY year_month, abs_value;'
            for row in cursor.execute(query):
                g1_count = row[0]
                g1_value = row[1]
                year_month = row[2]

                parameters.append((year_month, 'flags', 'highest_flag', 'name', g1_value, None, None, None, g1_count, None))
            cursor.executemany(query_cm,parameters)
            conn.commit()


        # flags   highest_flag  name    sysop, autopatrolled, bureaucrat, etc.                
        parameters = []
        highest_flag_count = {}
        year_month = cycle_year_month
        query = 'SELECT count(user_id), highest_flag FROM '+languagecode+'wiki_editors GROUP by highest_flag;'
        for row in cursor.execute(query):
            g1_count = row[0]
            g1_value = row[1]
            highest_flag_count[g1_value] = g1_count

            parameters.append((year_month, 'flags', 'highest_flag', 'name', g1_value, None, None, None, g1_count, None))
        cursor.executemany(query_cm,parameters)
        conn.commit()


        # flags   highest_flag  name    sysop, autopatrolled, bureaucrat, etc.  monthly_edits   threshold   5
        parameters = []
        query = 'SELECT count(e1.user_id), e1.highest_flag, e2.year_month FROM '+languagecode+'wiki_editors e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id WHERE e2.metric_name = "monthly_edits" AND e2.abs_value >= 5 GROUP BY e1.highest_flag, e2.year_month ORDER BY e1.highest_flag, e2.year_month ASC;'
        for row in cursor.execute(query):
            g1_count = highest_flag_count[g1_value]
            g2_count = row[0]
            g1_value = row[1]
            year_month = row[2]

            parameters.append((year_month, 'flags', 'highest_flag', 'name', g1_value, 'monthly_edits', 'threshold', 5, g1_count, g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()


        # flags   highest_flag  name    sysop, autopatrolled, bureaucrat, etc.  highest_flag_year_month bin 2001-2021           x: g1, y: g2 (last year_month)
        # flags   highest_flag  name    sysop, autopatrolled, bureaucrat, etc.  year_first_edit bin 2001-2021      
        # flags   highest_flag  name    sysop, autopatrolled, bureaucrat, etc.  lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021
        g2_variables = ['highest_flag_year_month', 'year_first_edit','lustrum_first_edit']

        for g2 in g2_variables:
            parameters = []
            query = 'SELECT count(user_id), highest_flag, '+g2+' FROM '+languagecode+'wiki_editors GROUP BY highest_flag, '+g2
            year_month = cycle_year_month
            for row in cursor.execute(query):
                g2_count = row[0]

                g1_value = row[1]
                g2_value = row[2]
                g1_count = highest_flag_count[g1_value]

                parameters.append((year_month, 'flags', 'highest_flag', 'name',  g1_value, g2, 'bin', g2_value, g1_count, g2_count))
            cursor.executemany(query_cm,parameters)
            conn.commit()



        editing_days = {(1,100):'1-100',(101,500):'101-500', (501,1000):'501-1000', (1001,1500):'1001-1500', (1501,2500):'1501-2500', (2501,3500):'2501-3500', (3501,4500):'3501-4500', (4501,5500):'4501-5500', (5501,6500):'5501-6500', (6501,7500):'6501-7500'}

        percent_editing_days = {(0,10):'0-10',(11,20):'11-20',(21,30):'21-30',(31,40):'31-40',(41,50):'41-50',(51,60):'51-60',(61,70):'61-70',(71,80):'71-80',(81,90):'81-90',(91,100):'91-100'}

        active_months = {(0,0):'0', (217, 228): '217-228', (301, 312): '301-312', (277, 288): '277-288', (25, 36): '25-36', (241, 252): '241-252', (109, 120): '109-120', (85, 96): '85-96', (61, 72): '61-72', (205, 216): '205-216', (289, 300): '289-300', (193, 204): '193-204', (73, 84): '73-84', (49, 60): '49-60', (37, 48): '37-48', (265, 276): '265-276', (181, 192): '181-192', (145, 156): '145-156', (13, 24): '13-24', (253, 264): '253-264', (133, 144): '133-144', (1, 12): '1-12', (121, 132): '121-132', (169, 180): '169-180', (157, 168): '157-168', (229, 240): '229-240', (97, 108): '97-108'}

        bin_dicts = {'editing_days':editing_days, 'percent_editing_days':percent_editing_days}


        # flags   highest_flag    name    sysop, autopatrolled, bureaucrat, etc.  editing_days    bin 1-100, 100-200, etc.        
        # flags   highest_flag    name    sysop, autopatrolled, bureaucrat, etc.  percent_editing_days    bin 1-10 to 100     
        for variable_name, bin_dict in bin_dicts.items():
            parameters = []
            for interval, label in bin_dict.items():
                query = 'SELECT count(e1.user_id), e1.abs_value, e1.year_month, "'+label+'" FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "edit_count_bin" AND e2.'+variable_name+' BETWEEN '+str(interval[0])+' AND '+str(interval[1])+' GROUP by e1.abs_value;'

                g2_count = row[0]
                g1_value = row[1]
                g2_value = label
                year_month = row[2]
                g1_count = highest_flag_count[g1_value]

                parameters.append((year_month,  'flags', 'highest_flag', 'name', g1_value, variable_name, 'bin', g2_value, g1_count, g2_count))
                cursor.executemany(query_cm,parameters)
                conn.commit()


        # flags   highest_flag    name    sysop, autopatrolled, bureaucrat, etc.  active_months   bin 1-10, 10-20, 30-40,... to 150          
        for interval, label in active_months.items():
            query = 'SELECT count(e1.user_id), e1.abs_value, e1.year_month, "'+label+'" FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "edit_count_bin" AND e2.metric_name = "active_months" AND e2.abs_value BETWEEN '+str(interval[0])+' AND '+str(interval[0])+' GROUP by e1.abs_value;'

            g2_count = row[0]
            g1_value = row[1]
            g2_value = label
            year_month = row[2]
            g1_count = highest_flag_count[g1_value]

            parameters.append((year_month,  'flags', 'highest_flag', 'name', g1_value, "active_months", 'bin', g2_value, g1_count, g2_count))
            cursor.executemany(query_cm,parameters)
            conn.commit()



        # flags   highest_flag  name    sysop, autopatrolled, bureaucrat, etc.  total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000
        parameters = []

        query = 'SELECT count(e1.user_id), e1.highest_flag, e2.abs_value, e2.year_month FROM '+languagecode+'wiki_editors e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id WHERE e2.metric_name = "edit_count_bin" GROUP BY e1.highest_flag, e2.abs_value;'

        for row in cursor.execute(query):
            g1_count = highest_flag_count[g1_value]
            g2_count = row[0]
            g1_value = row[1]
            g2_value = row[2]
            year_month = row[3]
            parameters.append((year_month, 'flags', 'highest_flag', 'name', g1_value, 'total_edits', 'bin', g2_value, g1_count, g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()

        print ('flags')



    # ACTIVE CONTRIBUTORS (THRESHOLD, NO BINS)
    # active_editors, active_editors_5, active_editors_10, active_editors_50, active_editors_100, active_editors_500, active_editors_1000
    def active_editors():
    
        # active_editors    monthly_edits   threshold   1, 5, 10, 50, 100, 500, 1000
        active_editors_5_year_month = {}
        values = [1,5,10,50,100,500,1000,5000]
        parameters = []
        for v in values:
            query = 'SELECT count(distinct user_id), year_month FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" AND abs_value >= '+str(v)+' GROUP BY year_month ORDER BY year_month'
            for row in cursor.execute(query):
                # print (row)
                g1_count=row[0];
                year_month=row[1]
                if year_month == '': continue
                parameters.append((year_month, 'active_editors', 'monthly_edits', 'threshold', v, None, None, None, g1_count, None))
                if v == 5: active_editors_5_year_month[year_month] = g1_count
        cursor.executemany(query_cm,parameters)
        conn.commit()


        # active_editors    monthly_edits   bin 1, 5, 10, 50, 100, 500, 1000
        parameters = []
        values = [1,5,10,50,100,500,1000,5000]
        for x in range(0,len(values)):
            v = values[x]
            if x < len(values)-1: 
                w = values[x+1]
                query = 'SELECT count(distinct user_id), year_month FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" AND abs_value >= '+str(v)+' AND abs_value < '+str(w)+' GROUP BY year_month ORDER BY year_month'
                w = w - 1
            else:
                w = 'inf'
                query = 'SELECT count(distinct user_id), year_month FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" AND abs_value >= '+str(v)+' GROUP BY year_month ORDER BY year_month'
            # print (query)
            for row in cursor.execute(query):
                # print (row)
                g1_count=row[0];
                year_month=row[1]
                if year_month == '': continue
                parameters.append((year_month, 'active_editors', 'monthly_edits', 'bin', str(v)+'_'+str(w) , None, None, None, g1_count, None))
        cursor.executemany(query_cm,parameters)
        conn.commit()


        # active_editors    monthly_edits   threshold   5   year_first_edit bin 2001-2021
        query = 'SELECT count(e1.user_id), e1.year_month, e2.year_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= 5 GROUP BY e1.year_month, e2.year_first_edit;'
        parameters = []
        for row in cursor.execute(query):
            # print (row)
            g2_count=row[0];
            year_month=row[1]
            year_first_edit=row[2]

            if year_month == '': continue
            parameters.append((year_month, 'active_editors', 'monthly_edits', 'bin', 5, 'year_first_edit', 'bin', year_first_edit, active_editors_5_year_month[year_month], g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()


        # active_editors    monthly_edits   threshold   5   lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021
        query = 'SELECT count(e1.user_id), e1.year_month, e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= 5 GROUP BY e1.year_month, e2.lustrum_first_edit;'
        parameters = []
        for row in cursor.execute(query):
            # print (row)
            g2_count=row[0];
            year_month=row[1]
            lustrum_first_edit=row[2]

            if year_month == '': continue
            parameters.append((year_month, 'active_editors', 'monthly_edits', 'bin', 5, 'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_5_year_month[year_month], g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()


        # active_editors  monthly_edits   threshold   5   active_months   bin 1-10, 10-20, 30-40,... to 150
        active_months = {(0,0):'0', (217, 228): '217-228', (301, 312): '301-312', (277, 288): '277-288', (25, 36): '25-36', (241, 252): '241-252', (109, 120): '109-120', (85, 96): '85-96', (61, 72): '61-72', (205, 216): '205-216', (289, 300): '289-300', (193, 204): '193-204', (73, 84): '73-84', (49, 60): '49-60', (37, 48): '37-48', (265, 276): '265-276', (181, 192): '181-192', (145, 156): '145-156', (13, 24): '13-24', (253, 264): '253-264', (133, 144): '133-144', (1, 12): '1-12', (121, 132): '121-132', (169, 180): '169-180', (157, 168): '157-168', (229, 240): '229-240', (97, 108): '97-108'}

        parameters = []
        for interval, label in active_months.items():
            query = 'SELECT count(e1.user_id), e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= 5 AND e2.metric_name = "active_months" AND e2.abs_value BETWEEN '+str(interval[0])+' AND '+str(interval[1])+' GROUP by e1.year_month, e1.abs_value;'

            for row in cursor.execute(query):

                g2_count = row[0]
                year_month = row[1]
                g2_value = label

                g1_count = active_editors_5_year_month[year_month]
                parameters.append((year_month, 'active_editors', 'monthly_edits', 'bin', 5, "active_months", 'bin', g2_value, g1_count, g2_count))
            cursor.executemany(query_cm,parameters)
            conn.commit()


        # active_editors  monthly_edits   threshold   5   active_months_row   bin 2, 3, 4, 5, …
        # active_editors  monthly_edits   threshold   5   max_active_months_row   bin 2, 3, 4, 5, …
        # active_editors  monthly_edits   threshold   5   inactive_months_row bin -1, 0, 1, 2, 3, 4, 5, … 12, …
        # active_editors    monthly_edits   threshold   5   max_inactive_months_row bin 2, 3, 4, 5, …       
        g2_variables = ['inactivity_periods','active_months_row', 'inactive_months_row','max_active_months_row','max_inactive_months_row']

        for g2_variable in g2_variables:
            parameters = []
            query = 'SELECT count(e1.user_id), e2.abs_value, e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= 5 AND e2.metric_name = "'+g2_variable+'" GROUP BY e1.year_month, e2.abs_value;'

            for row in cursor.execute(query):
                g2_count = row[0]
                g2_value = row[1]
                year_month = row[2]
                g1_count = active_editors_5_year_month[year_month]

                parameters.append((year_month, 'active_editors', 'monthly_edits', 'bin', 5, g2_variable, 'bin', g2_value, g1_count, g2_count))
            cursor.executemany(query_cm,parameters)
            conn.commit()


        # active_editors    monthly_edits   threshold   5   total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000
        parameters = []

        query = 'SELECT count(e1.user_id), e2.abs_value, e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= 5 AND e2.metric_name = "edit_count_bin" GROUP BY e2.abs_value;'

        for row in cursor.execute(query):
            g2_count = row[0]
            g2_value = row[1]
            year_month = row[2]
            g1_count = active_editors_5_year_month[year_month]

            parameters.append((year_month, 'active_editors', 'monthly_edits', 'bin', 5, 'total_edits', 'bin', g2_value, g1_count, g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()

        # active_editors    monthly_edits   threshold   5   flag    name    sysop, autopatrolled, bureaucrat, etc.
        parameters = []
        query = 'SELECT count(e1.user_id), e1.abs_value, e2.highest_flag, e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= 5 GROUP by e1.abs_value, e2.highest_flag;'
        for row in cursor.execute(query):
            g2_count = row[0]
            g1_value = row[1]
            g2_value = row[2]
            year_month = row[3]
            g1_count = active_editors_5_year_month[year_month]

            parameters.append((year_month, 'retained_', 'total_edits', 'bin', g1_value, 'highest_flag', 'name', g2_value, g1_count, g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()


        # active_editors    monthly_edits   threshold   5   monthly_editing_days    bin 1-10 to 100
        parameters = []
        query = 'SELECT count(e1.user_id), e2.abs_value, e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= 5 AND e2.metric_name = "monthly_editing_days" GROUP BY e1.year_month, e2.abs_value;'

        for row in cursor.execute(query):
            g2_count = row[0]
            g2_value = row[1]
            year_month = row[2]
            g1_count = active_editors_5_year_month[year_month]

            parameters.append((year_month, 'active_editors', 'monthly_edits', 'bin', 5, 'monthly_editing_days', 'bin', g2_value, g1_count, g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()

        print ('active_editors')





    def retention():

        # monthly_registered_first_edit
        parameters = []
        registered_baseline = {}
        query = 'SELECT count(distinct user_id), year_month_registration FROM '+languagecode+'wiki_editors GROUP BY 2 ORDER BY 2 ASC;'
        for row in cursor.execute(query):
            value=row[0];
            year_month=row[1]
            if year_month == '': continue
            try: registered_baseline[year_month] = int(value)
            except: pass
            parameters.append((year_month, 'registered_editors', 'register', 'threshold', 1, None, None, None, value, None))

        retention_baseline = {}
        query = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editors GROUP BY 2 ORDER BY 2 ASC;'
        for row in cursor.execute(query):
            value=row[0];
            year_month=row[1]
            if year_month == '': continue

            try: retention_baseline[year_month] = int(value)
            except: pass

            parameters.append((year_month, 'registered_editors', 'first_edit', 'threshold', 1, None, None, None, value, None))

            try:
                g1_count = registered_baseline[year_month]
            except:
                g1_count = 0

            parameters.append((year_month, 'registered_editors', 'register', 'threshold', 1, 'first_edit', 'threshold', 1, g1_count, value))


        cursor.executemany(query_cm,parameters)
        conn.commit()



        parameters = []
        queries_retention_dict = {}

        # RETENTION
        # number of editors who edited at least once 24h after the first edit
        queries_retention_dict['24h'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_24h" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 7 days after the first edit
        queries_retention_dict['7d'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_7d" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 30 days after the first edit
        queries_retention_dict['30d'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_30d" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 60 days after the first edit
        queries_retention_dict['60d'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_60d" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 365 days after the first edit
        queries_retention_dict['365d'] = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editors WHERE lifetime_days >= 365 GROUP BY 2 ORDER BY 1;'

        # number of editors who edited at least once 730 days after the first edit
        queries_retention_dict['730d'] = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editors WHERE lifetime_days >= 730 GROUP BY 2 ORDER BY 1;'


        for metric_name, query in queries_retention_dict.items():
            for row in cursor.execute(query):
                value=row[0];
                year_month=row[1]
                if year_month == '': continue
              


                try: g1_count = retention_baseline[year_month]
                except: g1_count = 0
                parameters.append((year_month, 'registered_editors', 'first_edit', 'threshold', 1, 'edited_after_time', 'threshold', metric_name, g1_count, value))



                try: g1_count = registered_baseline[year_month]
                except: g1_count = 0
                parameters.append((year_month, 'registered_editors', 'register', 'threshold', 1, 'edited_after_time', 'threshold', metric_name, g1_count, value))


        cursor.executemany(query_cm,parameters)
        conn.commit()

        parameters = []
        queries_retention_dict = {}


        # USER PAGES
        # number of editors who edited their user_page at least once during the first 24h after their first edit
        queries_retention_dict['editors_edited_user_page_d24h_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_edit_count_24h" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited their user_page at least once during the first 30 days after their first edit
        queries_retention_dict['editors_edited_user_page_d30d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_edit_count_1month" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited their user_page at least once
        queries_retention_dict['editors_edited_user_page_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "monthly_edits_ns2_user" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'


        for metric_name, query in queries_retention_dict.items():
            for row in cursor.execute(query):
                value=row[0];
                year_month=row[1]
                if year_month == '': continue
              
                try: g1_count = retention_baseline[year_month]
                except: g1_count = 0
                parameters.append((year_month, 'registered_editors', 'first_edit', 'threshold', 1, 'edited_user_page_after_time', 'threshold', metric_name, g1_count, value))

                try: g1_count = registered_baseline[year_month]
                except: g1_count = 0
                parameters.append((year_month, 'registered_editors', 'register', 'threshold', 1, 'edited_user_page_after_time', 'threshold', metric_name, g1_count, value))



        cursor.executemany(query_cm,parameters)
        conn.commit()

        parameters = []
        queries_retention_dict = {}



        # USER PAGE TALK PAGE
        # number of editors who edited their user_page_talk_page at least once during the first  24h after their first edit
        queries_retention_dict['editors_edited_user_page_talk_page_d24h_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_talk_page_edit_count_24h" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited their user_page_talk_page at least once during the first  30 daysafter their first edit
        queries_retention_dict['editors_edited_user_page_talk_page_d30d_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "user_page_talk_page_edit_count_1month" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited their user_page_talk_page at least once after the first edit
        queries_retention_dict['editors_edited_user_page_talk_page_afe'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "monthly_edits_ns3_user_talk" AND ce.abs_value > 0 GROUP BY 2 ORDER BY 2 ASC;'

        for metric_name, query in queries_retention_dict.items():
            for row in cursor.execute(query):
                value=row[0];
                year_month=row[1]
                if year_month == '': continue
              
                try: g1_count = retention_baseline[year_month]
                except: g1_count = 0
                parameters.append((year_month, 'registered_editors', 'first_edit', 'threshold', 1, 'edited_user_page_talk_page_after_time', 'threshold', metric_name, g1_count, value))

                try: g1_count = registered_baseline[year_month]
                except: g1_count = 0
                parameters.append((year_month, 'registered_editors', 'register', 'threshold', 1, 'edited_user_page_talk_page_after_time', 'threshold', metric_name, g1_count, value))

        cursor.executemany(query_cm,parameters)
        conn.commit()

        print ('retention')





    def drop_off():

        year_month = cycle_year_month

        lustrum_first_edit_dict = {}
        query = 'SELECT count(user_id), lustrum_first_edit FROM '+languagecode+'wiki_editors WHERE lustrum_first_edit != "" GROUP BY lustrum_first_edit;'
        for row in cursor.execute(query):
            lustrum_first_edit_dict[row[1]]=row[0]

        year_first_edit_dict = {}
        query = 'SELECT count(user_id), year_first_edit FROM '+languagecode+'wiki_editors WHERE year_first_edit != "" GROUP BY year_first_edit;'
        for row in cursor.execute(query):
            year_first_edit_dict[row[1]]=row[0]

        edit_bins_count = {}
        query = 'SELECT count(user_id), abs_value, year_month FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "edit_count_bin" GROUP by abs_value;'
        for row in cursor.execute(query):
            edit_bins_count[row[1]] = row[0]

        highest_flag_dict = {}
        query = 'SELECT count(user_id), highest_flag FROM '+languagecode+'wiki_editors WHERE highest_flag != "" GROUP BY highest_flag;'
        for row in cursor.execute(query):
            highest_flag_dict[row[1]]=row[0]


        # registered_editors    lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2020    year_last_edit  bin 2001-2021 (180 days since last edit)  
        parameters = []
        query = 'SELECT count(user_id), lustrum_first_edit, year_last_edit FROM '+languagecode+'wiki_editors WHERE lustrum_first_edit != "" AND days_since_last_edit >= 180  GROUP BY lustrum_first_edit, year_last_edit ORDER BY lustrum_first_edit, year_last_edit;'

        for row in cursor.execute(query):
            g2_count = row[0]
            g1_value = row[1]
            g2_value = row[2]

            parameters.append((year_month, 'registered_editors', 'lustrum_first_edit', 'bin', g1_value, 'year_last_edit', 'bin', g2_value, lustrum_first_edit_dict[g1_value], g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()     
        

        # registered_editors    year_first_edit bin 2001-2021   year_last_edit  bin 2001-2021 (180 days since last edit)
        parameters = []
        query = 'SELECT count(user_id), year_first_edit, year_last_edit FROM '+languagecode+'wiki_editors WHERE year_first_edit != "" AND days_since_last_edit >= 180  GROUP BY year_first_edit, year_last_edit ORDER BY year_first_edit, year_last_edit;'

        for row in cursor.execute(query):
            g2_count = row[0]
            g1_value = row[1]
            g2_value = row[2]

            parameters.append((year_month, 'registered_editors', 'year_first_edit', 'bin', g1_value, 'year_last_edit', 'bin', g2_value, year_first_edit_dict[g1_value], g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()     
        

        # participative_editors total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000    year_last_edit bin 2001-2021       
        parameters = []
        query = 'SELECT count(e1.user_id), e1.abs_value, e2.year_last_edit, e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "edit_count_bin" GROUP by e1.abs_value, e2.year_last_edit;'
        for row in cursor.execute(query):
            g2_count = row[0]
            g1_value = row[1]
            g2_value = row[2]
            g1_count = edit_bins_count[g1_value]
            year_month = row[3]
            parameters.append((year_month, 'participative_editors', 'total_edits', 'bin', g1_value, 'year_last_edit', 'bin', g2_value, g1_count, g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()


        # participative_editors total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000    over_past_max_inactive_months_row    threshold                 
        # participative_editors total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000    over_edit_bin_average_past_max_inactive_months_row    threshold    
        g2_variables = ['over_past_max_inactive_months_row','over_edit_bin_average_past_max_inactive_months_row','over_monthly_edit_bin_average_past_max_inactive_months_row']
        for g2_variable in g2_variables:
            parameters = []
            query = 'SELECT count(e1.user_id), e1.abs_value FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "edit_count_bin" AND e2.metric_name = "'+g2_variable+'" AND e2.abs_value > 0 GROUP by e1.abs_value;'
            for row in cursor.execute(query):
                g2_count = row[0]
                g1_value = row[1]
                g1_count = edit_bins_count[g1_value]
                parameters.append((year_month, 'participative_editors', 'total_edits', 'bin', g1_value, g2_variable, 'threshold', 0, g1_count, g2_count))
            cursor.executemany(query_cm,parameters)
            conn.commit()


        # registered_editors  lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2020    over_past_max_inactive_months_row   threshold   > 0
        # registered_editors  year_first_edit bin 2001-2021   over_edit_bin_average_past_max_inactive_months_row  threshold   > 0
        # registered_editors  lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2020    over_edit_bin_average_past_max_inactive_months_row  threshold   > 0
        # registered_editors  year_first_edit bin 2001-2021   over_past_max_inactive_months_row   threshold   > 0
        g1_variables = ['year_first_edit','lustrum_first_edit']
        g2_variables = ['over_past_max_inactive_months_row','over_edit_bin_average_past_max_inactive_months_row','over_monthly_edit_bin_average_past_max_inactive_months_row']
        for g1_variable in g1_variables:
            for g2_variable in g2_variables:

                parameters = []
                query = 'SELECT count(e1.user_id), e1.'+g1_variable+' FROM '+languagecode+'wiki_editors e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id WHERE e2.metric_name = "'+g2_variable+'" AND e2.abs_value > 0 GROUP by e1.'+g1_variable+';'
                for row in cursor.execute(query):
                    g2_count = row[0]
                    g1_value = row[1]

                    if g1_variable == 'year_first_edit':
                        try:
                            g1_count = year_first_edit_dict[g1_value]
                        except:
                            g1_count = 0
                    elif g1_variable == 'lustrum_first_edit':
                        try:
                            g1_count = lustrum_first_edit[g1_value]
                        except:
                            g1_count = 0
    
                    parameters.append((year_month, 'registered_editors', g1_variable, 'bin', g1_value, g2_variable, 'threshold', 0, g1_count, g2_count))
                cursor.executemany(query_cm,parameters)
                conn.commit()


        # participative_editors total_edits bin 0_100, 100_500, 500_1000, 1000_5000, 5000_10000, 10000_50000, 50000_100000, 100000_500000, 500000_1000000, 1000000_1000000000000    days_since_last_edit    bin   60, 120, 180.

        days_since_last_edit = 60
        while days_since_last_edit <= 1095: # 20 years = 7200 days

            parameters = []
            next_value_days_since_last_edit = days_since_last_edit + 60

            if next_value_days_since_last_edit < 1095:
                query = 'SELECT count(e1.user_id), e1.abs_value FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "edit_count_bin" AND days_since_last_edit BETWEEN '+str(days_since_last_edit)+' AND '+str(next_value_days_since_last_edit)+' GROUP by e1.abs_value;'
            else:
                query = 'SELECT count(e1.user_id), e1.abs_value FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "edit_count_bin" AND days_since_last_edit > '+str(days_since_last_edit)+' GROUP by e1.abs_value;'

            for row in cursor.execute(query):
                g2_count = row[0]
                g1_value = row[1]
                g1_count = edit_bins_count[g1_value]
                g2_value = days_since_last_edit
                parameters.append((year_month, 'participative_editors', 'total_edits', 'bin', g1_value, 'year_last_edit', 'bin', g2_value, g1_count, g2_count))

            cursor.executemany(query_cm,parameters)
            conn.commit()

            days_since_last_edit = next_value_days_since_last_edit



        # flags highest_flag    name    sysop, autopatrolled, bureaucrat, etc.  year_last_edit  bin 2001-2021 (180 days inactive since calculation)     
        parameters = []
        query = 'SELECT count(user_id), highest_flag, year_last_edit FROM '+languagecode+'wiki_editors WHERE days_since_last_edit >= 180 GROUP BY year_last_edit, highest_flag'
        year_month = cycle_year_month
        for row in cursor.execute(query):
            g2_count = row[0]
            g1_value = row[1]

            g2_value = row[2]

            try:
                g1_count = highest_flag_dict[g1_value]
            except:
                g1_count = 0

            parameters.append((year_month, 'flags', 'highest_flag', 'name',  g1_value, 'year_last_edit', 'bin', g2_value, g1_count, g2_count))
        cursor.executemany(query_cm,parameters)
        conn.commit()




        # active_editors  monthly_edits   threshold   5   monthly_edits_to_baseline   bin 10, 20, 30, 40, 50, 60, 70, 80, 90, 100
        # active_editors  monthly_edits   threshold   5   monthly_editing_days_to_baseline    bin 10, 20, 30, 40, 50, 60, 70, 80, 90, 100
        active_editors_5_year_month = {}
        query = 'SELECT count(distinct user_id), year_month FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" AND abs_value >= '+str(5)+' GROUP BY year_month ORDER BY year_month'
        for row in cursor.execute(query):
            active_editors_5_year_month[row[1]] = row[0]


        g2_variables = ['monthly_edits_to_baseline','monthly_editing_days_to_baseline']

        for g2_variable in g2_variables:
            query = 'SELECT count(e1.user_id), e1.year_month, e2.abs_value FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= 5 AND e2.metric_name = "'+g2_variable+'" GROUP by e1.year_month, e2.abs_value;'
            for row in cursor.execute(query):

                g2_count = row[0]
                year_month = row[1]
                g2_value = row[2]

                g1_count = active_editors_5_year_month[year_month]
                parameters.append((year_month, 'active_editors', 'monthly_edits', 'bin', 5, g2_variable, 'bin', g2_value, g1_count, g2_count))

        cursor.executemany(query_cm,parameters)
        conn.commit()

        print ('drop_off')






    def actions():

        year_month = cycle_year_month


        # monthly_edits monthly_edits   sum main, monthly_edits_ns0_main, etc.
        g1_variables = ['monthly_editing_days','monthly_edits','monthly_edits_ns0_main','monthly_edits_ns10_template','monthly_edits_ns11_template_talk','monthly_edits_ns12_help','monthly_edits_ns13_help_talk','monthly_edits_ns14_category','monthly_edits_ns15_category_talk','monthly_edits_ns1_talk','monthly_edits_ns2_user','monthly_edits_ns3_user_talk','monthly_edits_ns4_project','monthly_edits_ns5_project_talk','monthly_edits_ns6_file','monthly_edits_ns7_file_talk','monthly_edits_ns8_mediawiki','monthly_edits_ns9_mediawiki_talk']

        parameters = []
        sum_monthly_edits = {}
        for g1_variable in g1_variables:
            query = 'SELECT SUM(abs_value), year_month FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "'+g1_variable+'" GROUP BY year_month ORDER BY year_month'
            for row in cursor.execute(query):
                g1_count = row[0]
                sum_monthly_edits[g1_variable,row[1]] = g1_count
                parameters.append((year_month, 'monthly_edits', 'monthly_edits', 'sum', g1_variable, None, None, None, g1_count, None))

        cursor.executemany(query_cm,parameters)
        conn.commit()


        # edits
        g2_variables = ['lustrum_first_edit','year_first_edit','year_last_edit','highest_flag']
        for g2_variable in g2_variables:
            for g1_variable in g1_variables:
                if g2_variable == 'year_last_edit':
                    query = 'SELECT SUM(e1.abs_value), e2.'+g2_variable+', e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE metric_name = "'+g1_variable+'" AND days_since_last_edit >= 180 GROUP BY e1.year_month, e2.'+g2_variable+' ORDER BY e1.year_month, e2.'+g2_variable
                else:
                    query = 'SELECT SUM(e1.abs_value), e2.'+g2_variable+', e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE metric_name = "'+g1_variable+'" GROUP BY e1.year_month, e2.'+g2_variable+' ORDER BY e1.year_month, e2.'+g2_variable

                # print (g1_variable, g2_variable)
                # print (query)
                # input('')
                for row in cursor.execute(query):
                    g2_value = row[1]
                    g2_count = row[0]
                    year_month = row[2]
                    g1_count = sum_monthly_edits[g1_variable,year_month]

                    parameters.append((year_month, 'monthly_edits', 'monthly_edits', 'sum', g1_variable, g2_variable, 'bin', g2_value, g1_count, g2_count))

                # print (len(parameters))

        cursor.executemany(query_cm,parameters)
        conn.commit()



        # monthly_edits sum main, monthly_edits_ns0_main, etc.  active_months   bin 1-10, 10-20, 30-40,... to 150
        active_months = {(1,100):'0', (217, 228): '217-228', (301, 312): '301-312', (277, 288): '277-288', (25, 36): '25-36', (241, 252): '241-252', (109, 120): '109-120', (85, 96): '85-96', (61, 72): '61-72', (205, 216): '205-216', (289, 300): '289-300', (193, 204): '193-204', (73, 84): '73-84', (49, 60): '49-60', (37, 48): '37-48', (265, 276): '265-276', (181, 192): '181-192', (145, 156): '145-156', (13, 24): '13-24', (253, 264): '253-264', (133, 144): '133-144', (1, 12): '1-12', (121, 132): '121-132', (169, 180): '169-180', (157, 168): '157-168', (229, 240): '229-240', (97, 108): '97-108'}

        year_months = set()
        parameters = []
        for g1_variable in g1_variables:

            for interval, label in active_months.items():

                query = 'SELECT SUM(e1.abs_value), e1.year_month, "'+label+'" FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "'+g1_variable+'" AND e2.metric_name = "active_months" AND e2.abs_value BETWEEN '+str(interval[0])+' AND '+str(interval[1])+' GROUP by e1.year_month, e1.abs_value;'

                for row in cursor.execute(query):
                    g2_value = row[2]
                    g2_count = row[0]
                    year_month = row[1]
                    g1_count = sum_monthly_edits[g1_variable,year_month]
                    year_months.add(year_month)

                    parameters.append((year_month, 'monthly_edits', 'monthly_edits', 'sum', g1_variable, "active_months", 'bin', g2_value, g1_count, g2_count))

                cursor.executemany(query_cm,parameters)
                conn.commit()


        """


        # GINI
        def gini_calculation(x):
            # (Warning: This is a concise implementation, but it is O(n**2)
            # in time and memory, where n = len(x).  *Don't* pass in huge
            # samples!)

            # Mean absolute difference
            mad = np.abs(np.subtract.outer(x, x)).mean()
            # Relative mean absolute difference
            rmad = mad/np.mean(x)
            # Gini coefficient
            g = 0.5 * rmad
            return g

        parameters = []
        ym = sorted(list(year_months))
        for year_month in ym:
            query = 'SELECT ce.abs_value FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "monthly_edits" AND year_month="'+year_month+'" AND ch.bot = "editor" AND ce.abs_value > 0;'

            query = 'SELECT abs_value FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" AND year_month="'+year_month+'"'
            values = []
            for row in cursor.execute(query): values.append(row[0]);
            v = gini_calculation(values)

            parameters.append((year_month, 'monthly_edits', 'monthly_edits', 'gini', 'monthly_edits', None, None, None, v, None))


        # query = 'SELECT ce.abs_value FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count" AND ch.bot = "editor" AND ce.abs_value > 0;'
        # values = []
        # for row in cursor.execute(query): values.append(row[0]);
        # v = gini(values)
        # print (v)
        # parameters.append((v, 'gini_edits', year_month))

        # parameters.append((year_month, 'monthly_edits', 'monthly_edits', 'gini', 'monthly_edits', None, None, None, v, None))


        cursor.executemany(query_cm,parameters)
        conn.commit()

        """

        print ('actions')


    participation()
    flags()
    active_editors()
    retention()
    drop_off()
    actions()

    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print(languagecode+' '+ function_name+' '+ duration)


"""

def editor_metrics_social(languagecode):
    pass

Iteració sencera a MediaWiki history
Mètriques mensuals.

Iterar pel mes que anem. 
Consulta a cada mes als registrats d'aquell mes o dos abans.
Comprovar quants d'aquests s'hi interactua I fer els comptadors

Cal guardar les últimes edicions de tots els usuaris per comprovar si hi ha interacció. 


Fetes 
Interactions newcomers_user_page_talk_page_edits
Interactions newcomers_article_talk_page_edits
Interactions newcomer_count
Interactions survivors_count (aquesta sempre hi haurà un decalatge de dos mesos) -> quants dels newcomers amb qui has interactuat sobreviuen.

Interaccions rebudes
User talk pages
Article talk pages


Hipòtesi. Quan els editors estan a punt de fer drop off... Deixen abans d'interactuar amb newcomers. 

Hipòtesi. Quan els editors deixen d'interactuar amb ells... Estan més a prop del drop off. 




def editor_metrics_multilingual(languagecode):
    print('')
# * wiki_editors
# (user_id integer, user_name text, bot text, user_flags text, primarybinary, primarylang text, primarybinary_ecount, totallangs_ecount, numberlangs integer)

# FUNCTION
# multilingualism: això cal una funció que passi per les diferents bases de dades i creï aquesta


def editor_metrics_content_diversity(languagecode):
    print('')
#    https://stackoverflow.com/questions/28816330/sqlite-insert-if-not-exist-else-increase-integer-value

#    PER NO GUARDAR-HO TOT EN MEMÒRIA. FER L'INSERT DELS CCC EDITATS A CADA ARXIU.
# * wiki_editor_content_metrics

# (user_id integer, user_name text, content_type text, value real)

# FUNCTION
# això cal una funció que corri el mediawiki history amb aquest objectiu havent preseleccionat editors també.

    functionstartTime = time.time()
    function_name = 'editor_metrics_content_diversity '+languagecode
    print (function_name)
    print (languagecode)

    d_paths = get_mediawiki_paths(languagecode)

    if (len(d_paths)==0):
        print ('dump error. this language has no mediawiki_history dump: '+languagecode)
        # wikilanguages_utils.send_email_toolaccount('dump error at script '+script_name, dumps_path)
        # quit()

    for dump_path in d_paths:

        print(dump_path)
        iterTime = time.time()

        dump_in = bz2.open(dump_path, 'r')
        line = dump_in.readline()
        line = line.rstrip().decode('utf-8')[:-1]
        values = line.split(' ')

        parameters = []
        editors_params = []

        iter = 0
        while line != '':
            # iter += 1
            # if iter % 1000000 == 0: print (str(iter/1000000)+' million lines.')

            line = dump_in.readline()
            line = line.rstrip().decode('utf-8')[:-1]
            values = line.split('\t')

            if len(values)==1: continue

            page_id = values[23]
            page_title = values[25]
            page_namespace = int(values[28])
            edit_count = values[34]


Pel tema Edits A Ccc
Diccionari de diccionaris amb el què va editant cada editor cada mes. Més ràpid pel hash.

dict_editors {}
dict_CCC_per_editor {}

Els Edits mensuals a cada CCC? els anem col·locant a una bbdd, que pot ser la mateixa o una altra.
Després sumar l'acumulat final i ja està.
S'esborren els mensuals... Ja que és massa contingut. 


"""

def export_community_health_metrics_csv(languagecode):

    functionstartTime = time.time()
    function_name = 'export_community_health_metrics_csv '+languagecode
    print (function_name)

    cursor = datetime.datetime.strptime('2001-01','%Y-%m')
    final = datetime.datetime.strptime('2020-12','%Y-%m')

    list_months = []
    list_numerals = []
    months_numeral = {}
    numeral = 0
    while cursor < final:
        numeral += 1
        cursor_text = cursor.strftime('%Y-%m')
        months_numeral[cursor_text] = numeral
        cursor = (cursor + relativedelta.relativedelta(months=1))
        list_numerals.append(numeral)
        list_months.append(cursor_text)

    conn = sqlite3.connect(databases_path + community_health_metrics_db); cursor = conn.cursor()
    query = 'SELECT ee.user_id, ee.user_name, ee.value as edits, ec.year_month_registration, ec.lifetime_days, ec.last_edit_timestamp, ec.bot, ec.user_flags FROM '+languagecode+'wiki_editor_metrics ee INNER JOIN '+languagecode+'wiki_editors ec ON ee.user_id = ec.user_id WHERE metric_name = "edit_count";'
    df = pd.read_sql_query(query, conn)
    df = df.set_index('user_id')

    df1 = pd.concat([pd.DataFrame(columns=list_months),df], sort = True)
    df2 = pd.concat([pd.DataFrame(columns=list_numerals),df], sort = True)

    query = 'SELECT value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" ORDER BY user_name, year_month;'

    for row in cursor.execute(query):
        edits=row[0]
        current_year_month = row[1]
        current_year_month_numeral = months_numeral[current_year_month]
        cur_user_id = row[2]
        df1.at[cur_user_id, current_year_month] = edits
        df2.at[cur_user_id, current_year_month_numeral] = edits

    df1 = df1.fillna(0)
    df2 = df2.fillna(0)

    df1.to_csv('/srv/wcdo/datasets/ca_editors_monthly_participation_month.csv')
    df2.to_csv('/srv/wcdo/datasets/ca_editors_monthly_participation_numeral.csv')

    print (df1.head(10))
    print (df2.head(10))

    df3 = pd.concat([pd.DataFrame(columns=list_months),df], sort = True)
    df4 = pd.concat([pd.DataFrame(columns=list_numerals),df], sort = True)

    query = 'SELECT value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_average_seconds_between_edits" ORDER BY user_name, year_month;'

    for row in cursor.execute(query):
        seconds=row[0]
        current_year_month = row[1]
        current_year_month_numeral = months_numeral[current_year_month]
        cur_user_id = row[2]
        df3.at[cur_user_id, current_year_month] = seconds
        df4.at[cur_user_id, current_year_month_numeral] = seconds

    df3 = df3.fillna(0)
    df4 = df4.fillna(0)


    df3.to_csv('/srv/wcdo/datasets/ca_editors_monthly_idle_time_month.csv')
    df4.to_csv('/srv/wcdo/datasets/ca_editors_monthly_idle_time_numeral.csv')

    print (df3.head(10))
    print (df4.head(10))






#######################################################################################

class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("community_health_metrics.out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("community_health_metrics.err", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass


### MAIN:
if __name__ == '__main__':
    sys.stdout = Logger_out()
    sys.stderr = Logger_err()

    startTime = time.time()

    cycle_year_month = (datetime.date.today() - relativedelta.relativedelta(months=1)).strftime('%Y-%m')

    territories = wikilanguages_utils.load_wikipedia_languages_territories_mapping()
    languages = wikilanguages_utils.load_wiki_projects_information();

    wikilanguagecodes = sorted(languages.index.tolist())
    print ('checking languages Replicas databases and deleting those without one...')
    # Verify/Remove all languages without a replica database
    for a in wikilanguagecodes:
        if wikilanguages_utils.establish_mysql_connection_read(a)==None:
            wikilanguagecodes.remove(a)
    print (wikilanguagecodes)


    wikilanguagecodes = ['ca']

    
    print ('* Starting the COMMUNITY HEALTH METRICS '+cycle_year_month+' at this exact time: ' + str(datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
    main()

    finishTime = time.time()
    print ('* Done with the COMMUNITY HEALTH METRICS completed successfuly after: ' + str(datetime.timedelta(seconds=finishTime - startTime)))
    wikilanguages_utils.finish_email(startTime,'community_health_metrics.out', 'COMMUNITY HEALTH METRICS')
