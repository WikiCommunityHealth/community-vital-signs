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
import sqlite3
# files
import gzip
import zipfile
import bz2
import json
import csv
import codecs
# requests and others
import urllib
import webbrowser
import numpy as np
# data
import pandas as pd
import gc



# https://stats.wikimedia.org/#/all-projects
# https://meta.wikimedia.org/wiki/List_of_Wikipedias/ca
# https://meta.wikimedia.org/wiki/Research:Metrics#Volume_of_contribution
# https://meta.wikimedia.org/wiki/Research:Wikistats_metrics/Act

vital_signs_editors_db = 'vital_signs_editors.db'
vital_signs_web_db = 'vital_signs_web.db'



# MAIN
def main():


    # EDITORS
    create_vital_signs_editors_db()
    for languagecode in wikilanguagecodes_plusmeta:
        print ('*')
        print (languagecode)
        editor_metrics_dump_iterator(languagecode) # it fills the database cawiki_editors, cawiki_editor_metrics
        print ('dump iterator done.\n')

        editor_metrics_db_iterator(languagecode)
        print (languagecode+" done")

        print (wikilanguagecodes_plusmeta)
        print ('*')
    editor_metrics_primary_language_calculation()


    # VITAL SIGNS
    for languagecode in wikilanguagecodes_plusmeta:
        vital_signs_db_iterator(languagecode) # it fills the database cawiki_vital_signs_metrics
        print (languagecode)

    print ('done')




################################################################

# FUNCTIONS

def create_vital_signs_editors_db():

    wikilanguagecodes_plusmeta = ['meta']

    conn = sqlite3.connect(databases_path + vital_signs_editors_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + vital_signs_web_db); cursor2 = conn2.cursor()

    
    # EDITORS DB
    for languagecode in wikilanguagecodes_plusmeta:
        table_name = languagecode+'wiki_editors'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" ("+

            "user_id integer, user_name text, bot text, user_flags text, highest_flag text, highest_flag_year_month text, gender text, "+

            "primarybinary integer, primarylang text, edit_count integer, primary_ecount integer, totallangs_ecount integer, primary_year_month_first_edit text, primary_lustrum_first_edit text, numberlangs integer, "+

            "registration_date, year_month_registration, first_edit_timestamp text, year_month_first_edit text, year_first_edit text, lustrum_first_edit text, "+

            "survived60d text, last_edit_timestamp text, year_last_edit text, lifetime_days integer, days_since_last_edit integer, PRIMARY KEY (user_name))")
        cursor.execute(query)


        table_name = languagecode+'wiki_editor_metrics'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (user_id integer, user_name text, abs_value real, rel_value real, metric_name text, year_month text, timestamp text, PRIMARY KEY (user_id, metric_name, year_month, timestamp))")
        cursor.execute(query)


    # VITAL SIGNS DB
    table_name = 'vital_signs_metrics'
    try:
        cursor2.execute("DROP TABLE "+table_name+";")
    except:
        pass
    query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (langcode text, year_year_month text, year_month text, topic text, m1 text, m1_calculation text, m1_value text, m2 text, m2_calculation text, m2_value text, m1_count float, m2_count float, PRIMARY KEY (langcode, year_year_month, year_month, topic, m1, m1_calculation, m1_value, m2, m2_calculation, m2_value))")
    cursor2.execute(query)

    conn2.commit()




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
    cym_timestamp_dt = datetime.datetime.today().replace(day=1) #.strftime('%Y-%m-%d %H:%M:%S')

    # print (cym_timestamp_dt)
    # input('')

    if (len(d_paths)==0):
        print ('dump error. this language has no mediawiki_history dump: '+languagecode)
        # wikilanguages_utils.send_email_toolaccount('dump error at script '+script_name, dumps_path)
        # quit()

    conn = sqlite3.connect(databases_path + vital_signs_editors_db); cursor = conn.cursor()


    user_id_user_name_dict = {}
    user_id_bot_dict = {}
    user_id_user_groups_dict = {}

    editor_first_edit_timestamp = {}
    editor_registration_date = {}

    editor_last_edit_timestamp = {}

    editor_user_group_dict = {}
    editor_user_group_dict_timestamp = {}

    # for the survival part
    survived_dict = {}
    survival_measures = []
    user_id_edit_count = {}


    # for the monthly part
    editor_monthly_namespace_technical = {} # 8, 10
    editor_monthly_namespace_coordination = {} # 4, 12

    editor_monthly_edits = {}



    last_year_month = 0
    first_date = datetime.datetime.strptime('2001-01-01 01:15:15','%Y-%m-%d %H:%M:%S')


#    print (d_paths)
#    input('')

#    d_paths = d_paths[15:]


    for dump_path in d_paths:

        print('\n'+dump_path)
        iterTime = time.time()

        dump_in = bz2.open(dump_path, 'r')
        line = 'something'
        line = dump_in.readline()

        print ('year_month here: '+str(last_year_month))
        # input('')


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

            editor_edit_count[event_user_id] = values[21]


            event_user_groups = values[11]
            if event_user_groups != '':
                user_id_user_groups_dict[event_user_id] = event_user_groups


            # if event_user_text in ['Barcelona']:

            #     print ('*')
            #     print (event_user_text, ',', event_user_groups, ',', 'historical:', values[10])
            #     print (values)

            # if len(values)>37:
            #     user_text = values[38]
            #     if event_type == 'altergroups':
            #         print ('-')
            #         print (values[38])
            #         print ('user groups historical:',values[41])
            #         print ('user groups:',values[42])
            #         print (values)
            #         print ('-')


            page_namespace = values[28]


            if event_entity == 'user':

                user_text = str(values[38]) # this is target of the event

                if event_type == 'altergroups':

                    user_id = values[36]
                    cur_ug = values[41]

                    user_text = values[38]
                    if user_text != '': user_id_user_name_dict[user_id] = user_text


                    # editor_user_group_dict --> el que té fins ara.
                    # editor_user_group_dict_timestamp[user_id,event_timestamp] = [metric_name, change_[0], cur_ug] --> 

                    if cur_ug != '' and cur_ug != None:

                        try:
                            old_ug = editor_user_group_dict[user_id]
                            if ',' in old_ug: 
                                old_ug_list = old_ug.split(',')
                            else:
                                old_ug_list = [old_ug]

                        except:
                            old_ug_list = []
#                        print (old_ug_list)


                        if ',' in cur_ug:
                            cur_ug_list = cur_ug.split(',')
                        else:
                            cur_ug_list = [cur_ug]
#                        print (cur_ug_list)


                        i = 0
                        for x in cur_ug_list:
                            if x not in old_ug_list:
#                                print ('granted flag: '+x+' for user: '+user_text+' timestamp: '+ event_timestamp)
#                                input('')
                                event_ts = event_timestamp[:len(event_timestamp)-i] 
                                editor_user_group_dict_timestamp[user_id,event_ts] = ['granted_flag', x]
                                i += 1


                        for x in old_ug_list: 
                            if x not in cur_ug_list:
#                                print ('removed flag: '+x+' for user: '+user_text+' timestamp: '+ event_timestamp)
#                                input('')

                                event_ts = event_timestamp[:len(event_timestamp)-i] 
                                editor_user_group_dict_timestamp[user_id,event_ts] = ['removed_flag', x]
                                i += 1


                        editor_user_group_dict[user_id] = cur_ug


            event_is_bot_by = values[13]
            if event_is_bot_by != '':
                user_id_bot_dict[event_user_id] = event_is_bot_by
                # print (event_user_text, event_is_bot_by)

            event_user_is_anonymous = values[17]
            if event_user_is_anonymous == True or event_user_id == '': continue

            event_user_registration_date = values[18]
            event_user_creation_date = values[19]
            if event_user_id not in editor_registration_date: 
                if event_user_registration_date != '':
                    editor_registration_date[event_user_id] = event_user_registration_date
                elif event_user_creation_date != '':
                    editor_registration_date[event_user_id] = event_user_creation_date


            ####### ---------

            # MONTHLY EDITS COUNTER
            try: editor_monthly_edits[event_user_id] = editor_monthly_edits[event_user_id]+1
            except: editor_monthly_edits[event_user_id] = 1


            # MONTHLY NAMESPACES EDIT COUNTER
            if page_namespace == '4' or page_namespace == '12':
                try: editor_monthly_namespace_coordination[event_user_id] = editor_monthly_namespace_coordination[event_user_id]+1
                except: editor_monthly_namespace_coordination[event_user_id] = 1
            elif page_namespace == '8' or page_namespace == '10':
                try: editor_monthly_namespace_technical[event_user_id] = editor_monthly_namespace_technical[event_user_id]+1
                except: editor_monthly_namespace_technical[event_user_id] = 1





            #######---------    ---------    ---------    ---------    ---------    ---------    

            # CHECK MONTH CHANGE AND INSERT MONTHLY EDITS/NAMESPACES EDITS/SECONDS
            current_year_month = datetime.datetime.strptime(event_timestamp_dt.strftime('%Y-%m'),'%Y-%m')


            if last_year_month != current_year_month and last_year_month != 0:
                lym = last_year_month.strftime('%Y-%m')
                print ('change of month / new: ',current_year_month, 'old: ',lym)

                lym_sp = lym.split('-')
                ly = lym_sp[0]
                lm = lym_sp[1]

                lym_days = calendar.monthrange(int(ly),int(lm))[1]

                monthly_edits = []
                namespaces = []


                for user_id, edits in editor_monthly_edits.items():
                    monthly_edits.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits', lym, ''))


                for user_id, edits in editor_monthly_namespace_coordination.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_coordination', lym, ''))
                    except: pass

                for user_id, edits in editor_monthly_namespace_technical.items():
                    try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_technical', lym, ''))
                    except: pass

 

                for key, data in editor_user_group_dict_timestamp.items():
                    user_id = key[0]
                    timestamp = key[1]

                    metric_name = data[0]
                    flags = data[1]

                    try:
                        namespaces.append((user_id, user_id_user_name_dict[user_id], flags, None, metric_name, lym, timestamp))
                        # print ('#')
                        # print ((user_id, user_id_user_name_dict[user_id], flags, None, metric_name, lym, timestamp))
                        # print ('#')
                    except:
                        # print ('error.') 
                        # print (user_id)
                        # print ('...'+flags)

                        pass



                query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
                cursor.executemany(query,monthly_edits)
                cursor.executemany(query,namespaces)

                # print ('lengths: ')
                # print (len(user_id_user_name_dict))
                # print (len(editor_monthly_edits))
                # print (len(editor_monthly_namespace_coordination))
                # print (len(editor_monthly_namespace_technical))
                # print (len(editor_user_group_dict_timestamp))
                # print ('inserted')



                conn.commit()

                monthly_edits = []
                namespaces = []


                editor_monthly_namespace_coordination = {}
                editor_monthly_namespace_technical = {}

                editor_monthly_edits = {}
                editor_user_group_dict_timestamp = {}

        
            last_year_month = current_year_month
            # month change



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


                # at 7 days
                if event_timestamp_dt >= first_edit_timestamp_7days_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_7d', first_edit_timestamp_7days_dt.strftime('%Y-%m'),first_edit_timestamp_7days_dt.strftime('%Y-%m-%d %H:%M:%S')))

                # at 1 month
                if event_timestamp_dt >= first_edit_timestamp_1months_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_30d', first_edit_timestamp_1months_dt.strftime('%Y-%m'),first_edit_timestamp_1months_dt.strftime('%Y-%m-%d %H:%M:%S')))


                # at 2 months
                if event_timestamp_dt >= first_edit_timestamp_2months_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_60d', first_edit_timestamp_2months_dt.strftime('%Y-%m'),first_edit_timestamp_2months_dt.strftime('%Y-%m-%d %H:%M:%S')))
                    survived_dict[event_user_id]=event_user_text

                    try: del user_id_edit_count[event_user_id]
                    except: pass


            # USER PAGE EDIT COUNT, ADD ONE MORE EDIT.
            if event_user_id not in survived_dict:

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


        # MONTHLY EDITS/NAMESPACES INSERT (LAST ROUND)
        print (last_year_month)

        try:
            lym = last_year_month.strftime('%Y-%m')
        except:
            lym = ''

        if lym != cym and lym != '':

            monthly_edits = []
            for event_user_id, edits in editor_monthly_edits.items():
                monthly_edits.append((event_user_id, user_id_user_name_dict[event_user_id], edits, None, 'monthly_edits', lym, ''))

            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
            cursor.executemany(query,monthly_edits)
            conn.commit()


            namespaces = []
            for key, data in editor_user_group_dict_timestamp.items():
                user_id = key[0]
                timestamp = key[1]

                metric_name = data[0]
                flags = data[1]

                try:
                    namespaces.append((user_id, user_id_user_name_dict[user_id], flags, None, metric_name, lym, timestamp))
                except:
                    pass

            for user_id, edits in editor_monthly_namespace_coordination.items():
                try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_coordination', lym, ''))
                except: pass

            for user_id, edits in editor_monthly_namespace_technical.items():
                try: namespaces.append((user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_technical', lym, ''))
                except: pass


            query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
            cursor.executemany(query,monthly_edits)
            cursor.executemany(query,namespaces)
            conn.commit()


            namespaces = []
            monthly_edits = []

            editor_monthly_edits = {}
            editor_monthly_namespace_coordination = {}
            editor_monthly_namespace_technical = {}
            editor_user_group_dict_timestamp = {}





        # USER CHARACTERISTICS INSERT
        user_characteristics1 = []
        user_characteristics2 = []
        for user_id, user_name in user_id_user_name_dict.items():
            
            try: user_flags = user_id_user_groups_dict[user_id]
            except: user_flags = None

            try: bot = user_id_bot_dict[user_id]
            except: bot = 'editor'

            if user_id in survived_dict: survived60d = '1'
            else: survived60d = '0'


            try:
                edit_count = editor_edit_count[user_id]
            except:
                edit_count = None

            try: registration_date = editor_registration_date[user_id]
            except: registration_date = None
            
            if registration_date == None: # THIS IS SOMETHING WE "ASSUME" BECAUSE THERE ARE MANY ACCOUNTS WITHOUT A REGISTRATION DATE.
                try: registration_date = editor_first_edit_timestamp[user_id]
                except: registration_date = None

            if registration_date != '' and registration_date != None: year_month_registration = datetime.datetime.strptime(registration_date[:len(registration_date)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
            else: year_month_registration = None

            try: fe = editor_first_edit_timestamp[user_id]
            except: fe = None

            try: 
                le = editor_last_edit_timestamp[user_id]
                year_last_edit = datetime.datetime.strptime(le[:len(le)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y')

            except: 
                le = None
                year_last_edit = None


            if fe != None and fe != '':  
                year_month = datetime.datetime.strptime(fe[:len(fe)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
                year_first_edit = datetime.datetime.strptime(fe[:len(fe)-2],'%Y-%m-%d %H:%M:%S').strftime('%Y')

                if int(year_first_edit) >= 2001 < 2006: lustrum_first_edit = '2001-2005'
                if int(year_first_edit) >= 2006 < 2011: lustrum_first_edit = '2006-2010'
                if int(year_first_edit) >= 2011 < 2016: lustrum_first_edit = '2011-2015'
                if int(year_first_edit) >= 2016 < 2021: lustrum_first_edit = '2016-2020'
                if int(year_first_edit) >= 2021 < 2026: lustrum_first_edit = '2021-2025'

                fe_d = datetime.datetime.strptime(fe[:len(fe)-2],'%Y-%m-%d %H:%M:%S')
            else:
                year_month = None
                year_first_edit = None
                lustrum_first_edit = None
                fe_d = None


            if le != None:
                le_d = datetime.datetime.strptime(le[:len(le)-2],'%Y-%m-%d %H:%M:%S')
                days_since_last_edit = (cym_timestamp_dt - le_d).days
            else:
                le_d = None
                days_since_last_edit = None


            if fe != None and fe != '' and le != None: lifetime_days =  (le_d - fe_d).days
            else: lifetime_days = None
        

            user_characteristics1.append((user_id, user_name, registration_date, year_month_registration,  fe, year_month, year_first_edit, lustrum_first_edit))

 
            if le != None:
                user_characteristics2.append((bot, user_flags, le, year_last_edit, lifetime_days, days_since_last_edit, survived60d, edit_count, user_id, user_name))


            # if user_id == 296 or user_name == 'Jolle':
            #     print ((user_id, user_name, registration_date, year_month_registration,  fe, year_month, year_first_edit, lustrum_first_edit, survived60d))
            #     print ((bot, user_flags, le, year_last_edit, lifetime_days, days_since_last_edit, se, user_id, user_name))
            #     print (cym_timestamp_dt,le_d)
                # print (editor_last_edit_timestamp[user_id])


        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editors (user_id, user_name, registration_date, year_month_registration, first_edit_timestamp, year_month_first_edit, year_first_edit, lustrum_first_edit) VALUES (?,?,?,?,?,?,?,?);'
        cursor.executemany(query,user_characteristics1)
        conn.commit()

        query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editors (bot, user_flags, last_edit_timestamp, year_last_edit, lifetime_days, days_since_last_edit, survived60d, edit_count, user_id, user_name) VALUES (?,?,?,?,?,?,?,?,?,?);'
        cursor.executemany(query,user_characteristics2)
        conn.commit()

        query = 'UPDATE '+languagecode+'wiki_editors SET bot = ?, user_flags = ?, last_edit_timestamp = ?, year_last_edit = ?, lifetime_days = ?, days_since_last_edit = ?, survived60d = ?, edit_count = ?, user_id = ? WHERE user_name = ?;'
        cursor.executemany(query,user_characteristics2)
        conn.commit()

        print (len(user_characteristics1),len(user_characteristics2))


        user_characteristics1 = []
        user_characteristics2 = []
#        user_id_user_name_dict = {}

        # insert or ignore + update
        user_id_bot_dict = {}
        user_id_user_groups_dict = {}
        editor_last_edit_timestamp = {}
        editor_seconds_since_last_edit = {}
        editor_edit_count = {}

        # insert or ignore
        editor_first_edit_timestamp = {}
        editor_registration_date = {}




        # keep only pending monthly edits
        print ('total number of stored user_ids at the end of the dump: ')
        print (len(user_id_user_name_dict))

        user_id_user_name_dict2 = {}
        for k in editor_monthly_edits.keys():
            user_id_user_name_dict2[k]=user_id_user_name_dict[k]

        for k in editor_monthly_namespace_coordination.keys():
            user_id_user_name_dict2[k]=user_id_user_name_dict[k]

        for k in editor_monthly_namespace_technical.keys():
            user_id_user_name_dict2[k]=user_id_user_name_dict[k] 

        for k in editor_user_group_dict_timestamp.keys():
            user_id_user_name_dict2[k[0]]=user_id_user_name_dict[k[0]]

        print ('updated number of necessary user_ids: ')
        print (len(user_id_user_name_dict2))


        user_id_user_name_dict = user_id_user_name_dict2
        user_id_user_name_dict2 = {}


        # END OF THE DUMP!!!!
        print ('end of the dump.')
        print ('*')
        print (str(datetime.timedelta(seconds=time.time() - iterTime)))
#        input('')




    """
    # AGGREGATED METRICS (EDIT COUNTS)
    monthly_aggregated_metrics = {'monthly_edits':'edit_count'}
    conn2 = sqlite3.connect(databases_path + vital_signs_editors_db); cursor2 = conn2.cursor()
    for monthly_metric_name, metric_name in monthly_aggregated_metrics.items():
        edit_counts = []
        query = 'SELECT user_id, user_name, SUM(abs_value) FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "'+monthly_metric_name+'" GROUP BY 2;'
        for row in cursor2.execute(query):
            user_id = row[0]
            user_name = row[1]
            value = row[2]
            edit_counts.append((value, user_name, user_id))

    query = 'UPDATE '+languagecode+'wiki_editors SET edit_count = ? WHERE user_name = ? AND user_id = ?;'
    cursor2.executemany(query,edit_counts)
    conn2.commit()
    print ('Updated the editors table with the edit count.')
    """



    # FLAGS UPDATE
    # Getting the highest flag
    conn = sqlite3.connect(databases_path + vital_signs_editors_db); cursor = conn.cursor()
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
                    params.append((f, user_name))
                    user_id_flag[user_id]=f
                else:
                    f = list(highest_rank.keys())[0]
                    params.append((f, user_name))
                    user_id_flag[user_id]=f

        else:
            if user_flags in flag_ranks and 'bot' not in user_flags:
                params.append((user_flags, user_name))
                user_id_flag[user_id]=user_flags

    query = 'UPDATE '+languagecode+'wiki_editors SET highest_flag = ? WHERE user_name = ?;'
    cursor.executemany(query,params)
    conn.commit()
    print ('Updated the editors table with highest flag')





    # let's update the highest_flag_year_month
    query = 'SELECT year_month, user_id, user_name, abs_value FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "granted_flag";'
    params2 = []

    conn = sqlite3.connect(databases_path + vital_signs_editors_db); cursor = conn.cursor()

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
            params2.append((year_month,user_name))


    # print (params2)
    query = 'UPDATE '+languagecode+'wiki_editors SET highest_flag_year_month = ? WHERE user_name = ?;'
    cursor.executemany(query,params2)
    conn.commit()

    print ('Updated the editors table with the year month they obtained the highest flag.')
    # print(list(highest_flag.values()).count('bureaucrat'))

    # print ('stop highest flag year month'); input('stop');

    # If an editor has been granted the 'bot' flag, even if it has been taken away, it must be a flag.
    query = 'SELECT user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "granted_flag" AND abs_value LIKE "%bot";'
    params = []
    for row in cursor.execute(query):
        username = row[1]

        if 'bot' in username:
            bottype = 'name,group'
        else:
            bottype = 'group'
        params.append((bottype,username))

    query = 'UPDATE '+languagecode+'wiki_editors SET bot = ? WHERE user_name = ?;'
    cursor.executemany(query,params)
    conn.commit()

    print ('Updated the table with the bots from flag.')



    duration = str(datetime.timedelta(seconds=time.time() - functionstartTime))
    print(languagecode+' '+ function_name+' '+ duration)






def editor_metrics_db_iterator(languagecode):

    functionstartTime = time.time()
    function_name = 'editor_metrics_db_iterator '+languagecode
    print (function_name)

    d_paths, cym = get_mediawiki_paths(languagecode)
    cycle_year_month = cym
    print (cycle_year_month)
    conn = sqlite3.connect(databases_path + vital_signs_editors_db); cursor = conn.cursor()



    # MONTHLY EDITS LOOP
    query = 'SELECT abs_value, year_month, user_id, user_name FROM '+languagecode+'wiki_editor_metrics WHERE metric_name = "monthly_edits" ORDER BY user_name, year_month'
    # AND user_name in ("Toniher","Marcmiquel","Barcelona","TaronjaSatsuma","Kippelboy")

    # print (query)
    user_count = 0
    old_user_id = ''
    old_edits = None
    expected_year_month_dt = ''

    # parameters = []
    # editors_edits_baseline_parameters = []



    active_months_row = 0

    total_edits = []
    edits_increase_decrease = 0

    try: os.remove(databases_path +'temporary_editor_metrics.txt')
    except: pass

    edfile2 = open(databases_path+'temporary_editor_metrics.txt', "w")
    for row in cursor.execute(query):
        edits=row[0]
        current_year_month = row[1]
        cur_user_id = row[2]
        cur_user_name = row[3]


        if cur_user_id != old_user_id and old_user_id != '':
            active_months_row = 0


        current_year_month_dt = datetime.datetime.strptime(current_year_month,'%Y-%m')


        # here there is a change of month
        # if the month is not the expected one
        if expected_year_month_dt != current_year_month_dt and expected_year_month_dt != '' and old_user_id == cur_user_id:

            while expected_year_month_dt < current_year_month_dt:
                # print (expected_year_month_dt, current_year_month_dt)

                expected_year_month_dt = (expected_year_month_dt + relativedelta.relativedelta(months=1))

            active_months_row = 1

        else:
            active_months_row = active_months_row + 1

            if active_months_row > 1:

                edfile2.write(str(cur_user_id)+'\t'+cur_user_name+'\t'+str(active_months_row)+'\t'+" "+'\t'+"active_months_row"+'\t'+current_year_month+'\t'+" "+'\n')


        old_year_month = current_year_month
        expected_year_month_dt = (datetime.datetime.strptime(old_year_month,'%Y-%m') + relativedelta.relativedelta(months=1))

        old_user_id = cur_user_id
        old_user_name = cur_user_name

  #      print ('# update: ',old_user_id, old_user_name, active_months, max_active_months_row, max_inactive_months_row, total_months)
        # input('')

    cycle_year_month_dt = datetime.datetime.strptime(cycle_year_month,'%Y-%m')


    try:
        if current_year_month_dt == None:
            print ('The table is empty. ERROR.')
    except:
        return



    conn = sqlite3.connect(databases_path + vital_signs_editors_db); cursor = conn.cursor()

    a_file = open(databases_path+"temporary_editor_metrics.txt")
    editors_metrics_parameters = csv.reader(a_file, delimiter="\t", quotechar = '|')
    query = 'INSERT OR IGNORE INTO '+languagecode+'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
    cursor.executemany(query,editors_metrics_parameters)
    conn.commit()
    os.remove(databases_path +'temporary_editor_metrics.txt')
    editors_metrics_parameters = []


    print ('done with the monthly edits.')




# This creates the database for the website.

def vital_signs_db_iterator(languagecode):

    functionstartTime = time.time()
    function_name = 'vital_signs_db_iterator '+languagecode
    print (function_name)

    conn = sqlite3.connect(databases_path + vital_signs_editors_db); cursor = conn.cursor()
    conn2 = sqlite3.connect(databases_path + vital_signs_web_db); cursor2 = conn2.cursor()


    d_paths, cym = get_mediawiki_paths(languagecode)
    cycle_year_month = cym

    query_cm = 'INSERT OR IGNORE INTO vital_signs_metrics (langcode, year_year_month, year_month, topic, m1, m1_calculation, m1_value, m2, m2_calculation, m2_value, m1_count, m2_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?);'



    # VITAL SIGNS DB
    table_name = 'vital_signs_metrics'
    query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (langcode text, year_year_month text, year_month text, topic text, m1 text, m1_calculation text, m1_value text, m2 text, m2_calculation text, m2_value text, m1_count float, m2_count float, PRIMARY KEY (langcode, year_year_month, year_month, topic, m1, m1_calculation, m1_value, m2, m2_calculation, m2_value))")
    cursor2.execute(query)




    def retention():

       # monthly_registered_first_edit
        parameters = []
        registered_baseline = {}
        query = 'SELECT count(distinct user_id), year_month_registration FROM '+languagecode+'wiki_editors GROUP BY 2 ORDER BY 2 ASC;'
        for row in cursor.execute(query):
            value=row[0];
            year_month=row[1]
            if year_month == '' or year_month == None: continue
            try: registered_baseline[year_month] = int(value)
            except: pass
            parameters.append((languagecode,'ym', year_month, 'retention', 'register', 'threshold', 1, None, None, None, value, None))

        retention_baseline = {}
        query = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editors GROUP BY 2 ORDER BY 2 ASC;'
        for row in cursor.execute(query):
            value=row[0];
            year_month=row[1]
            if year_month == '' or year_month == None: continue

            try: retention_baseline[year_month] = int(value)
            except: pass

            parameters.append((languagecode,'ym', year_month, 'retention', 'first_edit', 'threshold', 1, None, None, None, value, None))

            try:
                m1_count = registered_baseline[year_month]
            except:
                m1_count = 0

            parameters.append((languagecode,'ym', year_month, 'retention', 'register', 'threshold', 1, 'first_edit', 'threshold', 1, m1_count, value))


        cursor2.executemany(query_cm,parameters)
        conn2.commit()



        parameters = []
        queries_retention_dict = {}

        # RETENTION
        # number of editors who edited at least once 24h after the first edit
        queries_retention_dict['24h'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_24h" AND ce.abs_value > 0 AND ch.bot = "editor" GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 7 days after the first edit
        queries_retention_dict['7d'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_7d" AND ce.abs_value > 0 AND ch.bot = "editor" GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 30 days after the first edit
        queries_retention_dict['30d'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_30d" AND ce.abs_value > 0 AND ch.bot = "editor" GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 60 days after the first edit
        queries_retention_dict['60d'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode+'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_60d" AND ce.abs_value > 0 AND ch.bot = "editor" GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 365 days after the first edit
        queries_retention_dict['365d'] = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editors WHERE lifetime_days >= 365 AND bot = "editor" GROUP BY 2 ORDER BY 2;'

        # number of editors who edited at least once 730 days after the first edit
        queries_retention_dict['730d'] = 'SELECT count(distinct user_id), year_month_first_edit FROM '+languagecode+'wiki_editors WHERE lifetime_days >= 730 AND bot = "editor" GROUP BY 2 ORDER BY 2;'


        for metric_name, query in queries_retention_dict.items():
            for row in cursor.execute(query):
                value=row[0];
                year_month=row[1]
                if year_month == '' or year_month == None: continue

              
                try: m1_count = retention_baseline[year_month]
                except: m1_count = 0
                parameters.append((languagecode,'ym', year_month, 'retention', 'first_edit', 'threshold', 1, 'edited_after_time', 'threshold', metric_name, m1_count, value))


                try: m1_count = registered_baseline[year_month]
                except: m1_count = 0
                parameters.append((languagecode,'ym', year_month, 'retention', 'register', 'threshold', 1, 'edited_after_time', 'threshold', metric_name, m1_count, value))

        cursor2.executemany(query_cm,parameters)
        conn2.commit()

    retention(); print ('retention');





    def stability_balance_special_global_flags_functions():


        # year month or year
        for t in ['ym','y']:


            # ACTIVE EDITORS
            # active_editors    monthly_edits   threshold   5, 100
            active_editors_5_year_month = {}
            active_editors_100_year_month = {}

            values = [5,100]
            parameters = []
            for v in values:

                if t == 'ym':
                    query = 'SELECT count(distinct e1.user_id), e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' GROUP BY e1.year_month ORDER BY e1.year_month;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4) FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' GROUP BY 2 ORDER BY 2;'


                for row in cursor.execute(query):
                    # print (row)
                    m1_count=row[0];
                    year_month=row[1]

                    if v == 5: active_editors_5_year_month[year_month] = m1_count
                    if v == 100: active_editors_100_year_month[year_month] = m1_count


                    if year_month == '' or year_month == None: continue

                    parameters.append((languagecode, t, year_month, 'active_editors', 'monthly_edits', 'threshold', v, None, None, None, m1_count, None))

            cursor2.executemany(query_cm,parameters)
            conn2.commit()

            # print (active_editors_5_year_month)
            # print (active_editors_100_year_month)
            # input('')

            # active_editors    monthly_edits   bin 1, 5, 10, 50, 100, 500, 1000
            parameters = []
            values = [1,5,10,50,100,500,1000,5000,10000]
            for x in range(0,len(values)):
                v = values[x]
                if x < len(values)-1: 
                    w = values[x+1]

                    if t == 'ym':
                        query = 'SELECT count(distinct e1.user_id), e1.year_month FROM '+languagecode+'wiki_editor_metrics  e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND metric_name = "monthly_edits" AND abs_value >= '+str(v)+' AND abs_value < '+str(w)+' GROUP BY e1.year_month ORDER BY e1.year_month' 
                    else:
                        query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4) FROM '+languagecode+'wiki_editor_metrics  e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND metric_name = "monthly_edits" AND abs_value >= '+str(v)+' AND abs_value < '+str(w)+' GROUP BY 2 ORDER BY 2' 


                    w = w - 1
                else:
                    w = 'inf'


                    if t == 'ym':
                        query = 'SELECT count(distinct e1.user_id), e1.year_month FROM '+languagecode+'wiki_editor_metrics  e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' GROUP BY e1.year_month ORDER BY e1.year_month;'
                    else:
                        query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4) FROM '+languagecode+'wiki_editor_metrics  e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' GROUP BY 2 ORDER BY 2;'



                # print (query)
                for row in cursor.execute(query):
                    # print (row)
                    m1_count=row[0];
                    year_month=row[1]
                    if year_month == '': continue
                    parameters.append((languagecode, t, year_month, 'active_editors', 'monthly_edits', 'bin', str(v)+'_'+str(w) , None, None, None, m1_count, None))

            cursor2.executemany(query_cm,parameters)
            conn2.commit()


            
            # STABILITY
            # active_editors  monthly_edits   threshold   5   active_months   bin 1-10, 10-20, 30-40,... to 150
            values = [5,100]
            parameters = []
            for v in values:      

                active_months_row = {(0,0):'0', (1, 1): '1', (2, 2): '2', (3, 6): '3-6', (7, 12): '7-12', (13, 24): '13-24', (25, 5000): '+24'}

                for interval, label in active_months_row.items():

                    if t == 'ym':
                        query = 'SELECT count(distinct e1.user_id), e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id INNER JOIN '+languagecode+'wiki_editors e3 ON e1.user_id = e3.user_id WHERE e3.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' AND e2.metric_name = "active_months_row" AND e2.abs_value BETWEEN '+str(interval[0])+' AND '+str(interval[1])+' GROUP by e1.year_month, e1.abs_value;'
                    else:
                        query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4) FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id  INNER JOIN '+languagecode+'wiki_editors e3 ON e1.user_id = e3.user_id WHERE e3.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' AND e2.metric_name = "active_months_row" AND e2.abs_value BETWEEN '+str(interval[0])+' AND '+str(interval[1])+' GROUP by 2;'


                    for row in cursor.execute(query):

                        m2_count = row[0]
                        year_month = row[1]
                        m2_value = label

                        m1_count = active_editors_5_year_month[year_month]

                        if year_month == '' or year_month == None: continue


                        if v == 5: 
                            parameters.append((languagecode, t, year_month, 'stability', 'monthly_edits', 'threshold', v, "active_months_row", 'bin', m2_value, active_editors_5_year_month[year_month], m2_count))

                        if v == 100: 
                            parameters.append((languagecode, t, year_month, 'stability', 'monthly_edits', 'threshold', v, "active_months_row", 'bin', m2_value, active_editors_100_year_month[year_month], m2_count))


            cursor2.executemany(query_cm,parameters)
            conn2.commit()




            # BALANCE 

            values = [5,100]
            parameters = []
            for v in values:

                # active_editors    monthly_edits   threshold   5   lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021

                if t == 'ym':
                    query = 'SELECT count(distinct e1.user_id), e1.year_month, e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY e1.year_month, e2.lustrum_first_edit;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY 2, 3;'                    


                for row in cursor.execute(query):
                    # print (row)
                    m2_count=row[0];
                    year_month=row[1]
                    lustrum_first_edit=row[2]

                    if year_month == '' or year_month == None: continue

                    if v == 5: 
                        parameters.append((languagecode, t, year_month, 'balance', 'monthly_edits', 'threshold', v, 'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_5_year_month[year_month], m2_count))
                    if v == 100: 
                        parameters.append((languagecode, t, year_month, 'balance', 'monthly_edits', 'threshold', v, 'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_100_year_month[year_month], m2_count))

            cursor2.executemany(query_cm,parameters)
            conn2.commit()




            # SPECIAL FUNCTIONS
            # TECHNICAL EDITORS

            values = [5,100]
            parameters = []
            for v in values:

                # active_editors    monthly_edits   threshold   5   lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021

                if t == 'ym':
                    query = 'SELECT count(distinct e1.user_id), e1.year_month, e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits_technical" AND e1.abs_value >= '+str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY e1.year_month, e2.lustrum_first_edit;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits_technical" AND e1.abs_value >= '+str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY 2, 3;'



                for row in cursor.execute(query):
                    # print (row)
                    m2_count=row[0];
                    year_month=row[1]
                    lustrum_first_edit=row[2]

                    if year_month == '' or year_month == None: continue

                    if v == 5: 
                        parameters.append((languagecode, t, year_month, 'technical_editors', 'monthly_edits_technical', 'threshold', v, 'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_5_year_month[year_month], m2_count))
                    if v == 100: 
                        parameters.append((languagecode, t, year_month, 'technical_editors', 'monthly_edits_technical', 'threshold', v, 'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_100_year_month[year_month], m2_count))

            cursor2.executemany(query_cm,parameters)
            conn2.commit()




            # COORDINATORS

            values = [5,100]
            parameters = []
            for v in values:

                # active_editors    monthly_edits   threshold   5   lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021

                if t == 'ym':
                    query = 'SELECT count(distinct e1.user_id), e1.year_month, e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits_coordination" AND e1.abs_value >= '+str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY e1.year_month, e2.lustrum_first_edit;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits_coordination" AND e1.abs_value >= '+str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY 2, 3;'

                for row in cursor.execute(query):
                    # print (row)
                    m2_count=row[0];
                    year_month=row[1]
                    lustrum_first_edit=row[2]

                    if year_month == '' or year_month == None: continue

                    if v == 5: 
                        parameters.append((languagecode, t, year_month, 'coordinators', 'monthly_edits_coordination', 'threshold', v, 'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_5_year_month[year_month], m2_count))
                    if v == 100: 
                        parameters.append((languagecode, t, year_month, 'coordinators', 'monthly_edits_coordination', 'threshold', v, 'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_100_year_month[year_month], m2_count))

            cursor2.executemany(query_cm,parameters)
            conn2.commit()





            # GLOBAL / PRIMARY
            # aquí hi falten els active editors totals

            values = [5,100]
            parameters = []
            for v in values:

                if t == 'ym':
                    query = 'SELECT count(distinct e1.user_id), e1.year_month, e2.primarylang FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' AND e2.bot = "editor" GROUP BY e1.year_month, e2.primarylang;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.primarylang FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' AND e2.bot = "editor" GROUP BY 2, 3'                    


                for row in cursor.execute(query):
                    # print (row)
                    m2_count=row[0];
                    year_month=row[1]
                    primarylang=row[2]

                    if year_month == '' or year_month == None: continue

                    if v == 5: 
                        parameters.append((languagecode, t, year_month, 'primary_editors', 'monthly_edits', 'threshold', v, 'primarylang', 'bin', primarylang, active_editors_5_year_month[year_month], m2_count))
                    if v == 100: 
                        parameters.append((languagecode, t, year_month, 'primary_editors', 'monthly_edits', 'threshold', v, 'primarylang', 'bin', primarylang, active_editors_100_year_month[year_month], m2_count))

            cursor2.executemany(query_cm,parameters)
            conn2.commit()





            # FLAGS AMONG ACTIVE EDITORS

            # active_editors    monthly_edits   threshold   5   flag    name    sysop, autopatrolled, bureaucrat, etc.
            values = [5,100]
            parameters = []
            for v in values:


                if t == 'ym':
                    query = 'SELECT count(distinct e1.user_id), e1.year_month, e2.highest_flag FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' AND e2.highest_flag IS NOT NULL AND e2.bot = "editor" GROUP BY e1.year_month, e2.highest_flag;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.highest_flag FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= '+str(v)+' AND e2.highest_flag IS NOT NULL AND e2.bot = "editor" GROUP BY 2, 3'


                for row in cursor.execute(query):
                    m2_count = row[0]
                    year_month = row[1]
                    m2_value = row[2]

                    if year_month == '' or year_month == None: continue

                    if v == 5: 
                        parameters.append((languagecode, t, year_month, 'flags', 'monthly_edits', 'threshold', 5, 'highest_flag', 'name', m2_value, active_editors_5_year_month[year_month], m2_count))
                    if v == 100: 
                        parameters.append((languagecode, t, year_month, 'flags', 'monthly_edits', 'threshold', 5, 'highest_flag', 'name', m2_value, active_editors_100_year_month[year_month], m2_count))


            cursor2.executemany(query_cm,parameters)
            conn2.commit()


    stability_balance_special_global_flags_functions(); print ('stability_balance_special_global_flags_functions');



    def administrators():

        parameters = []
        for metric_name in ['granted_flag','removed_flag','highest_flag']:

            if metric_name == 'highest_flag':
                query = 'SELECT count(distinct e1.user_id), e1.highest_flag, substr(e1.year_month_first_edit, 1, 4), e1.lustrum_first_edit FROM '+languagecode+'wiki_editors e1 WHERE e1.highest_flag IS NOT NULL AND e1.bot = "editor" GROUP BY 2, 3 ORDER BY 2, 3;'

            else:
                query = 'SELECT count(distinct e1.user_id), e1.abs_value, substr(e1.year_month, 1, 4), e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "'+metric_name+'" AND e2.bot = "editor" GROUP BY 2, 3, 4;'


            for row in cursor.execute(query):

                m2_count= row[0];
                m1_value = row[1]
                year_month= row[2]
                m2_value = row[3]
              
                parameters.append((languagecode, 'y', year_month, 'flags', metric_name, 'name', m1_value, 'lustrum_first_edit', 'bin', m2_value, None, m2_count))



        cursor2.executemany(query_cm,parameters)
        conn2.commit()


    administrators(); print ('administrators');






def editor_metrics_primary_language_calculation():

    conn = sqlite3.connect(databases_path + 'vital_signs_editors.db'); cursor = conn.cursor()


    query = ("CREATE TABLE IF NOT EXISTS allwiki_editors (lang text, user_name text, edit_count integer, year_month_first_edit text, lustrum_first_edit text, PRIMARY KEY (lang, user_name));")
    cursor.execute(query)

    for languagecode in wikilanguagecodes_plusmeta:
        query = 'INSERT INTO allwiki_editors SELECT "'+languagecode+'", user_name, edit_count, year_month_first_edit, lustrum_first_edit FROM '+languagecode+'wiki_editors WHERE user_name != "";'
        cursor.execute(query)
        conn.commit()
        print (languagecode)
    print ('Allwiki editors table filled')


    try: os.remove(databases_path +'temporary_editor_metrics.txt')
    except: pass
    edfile2 = open(databases_path+'temporary_editor_metrics.txt', "w")

    query = 'SELECT user_name, lang, edit_count, year_month_first_edit, lustrum_first_edit FROM allwiki_editors ORDER BY user_name, edit_count DESC;'


    numbereditors = 0
    totallangs_ecount = 0
    numberlangs = 0
    primarylang = ''
    primary_ecount = 0
    primary_year_month_first_edit = ''
    primary_lustrum_first_edit = ''


    old_user_name = ''


    for row in cursor.execute(query):
        user_name = row[0]
        lang = row[1]
        try: edit_count = int(row[2])  
        except: edit_count = 0
        
        try: year_month_first_edit = str(row[3])
        except: year_month_first_edit = ''

        try: lustrum_first_edit = str(row[4])
        except: lustrum_first_edit = ''


        if user_name != old_user_name and old_user_name != '':
            numbereditors+=1

            # if old_user_name == 'Marcmiquel':
            #     print (primarylang+'\t'+str(primary_ecount)+'\t'+str(totallangs_ecount)+'\t'+str(numberlangs)+'\t'+ primary_year_month_first_edit+'\t'+primary_lustrum_first_edit+'\t'+user_name+'\n')
            #     input('')

            try:
                edfile2.write(primarylang+'\t'+str(primary_ecount)+'\t'+str(totallangs_ecount)+'\t'+str(numberlangs)+'\t'+ primary_year_month_first_edit+'\t'+primary_lustrum_first_edit+'\t'+old_user_name+'\n')
            except:
                pass

            # choose whether to insert or not
            if numbereditors % 100000 == 0:
                print (numbereditors)
                print ((100*numbereditors/64609745))
                print ('\n')

            # clean
            totallangs_ecount = 0
            numberlangs = 0
            primarylang = ''
            primary_ecount = 0
            primary_year_month_first_edit = ''
            primary_lustrum_first_edit = ''


        if edit_count > 4 and lang != "meta":
            numberlangs+=1

        if (edit_count > primary_ecount and lang != "meta"): # by definition we do not consider that meta can be a primary language. it is not a wikipedia.
            primarylang = lang
            primary_ecount = edit_count

            primary_year_month_first_edit = year_month_first_edit
            primary_lustrum_first_edit = lustrum_first_edit

        if primarylang == '' and lang != "meta":
            primarylang = lang


        totallangs_ecount+=edit_count

        old_user_name = user_name
        old_lang = lang


    edfile2.write(primarylang+'\t'+str(primary_ecount)+'\t'+str(totallangs_ecount)+'\t'+str(numberlangs)+'\t'+ primary_year_month_first_edit+'\t'+primary_lustrum_first_edit+'\t'+user_name+'\n')

    print ('All in the txt file')

    query = "DROP TABLE allwiki_editors;"
    cursor.execute(query)
    conn.commit()
    print ('Allwiki editors table deleted')


    ###
    conn = sqlite3.connect(databases_path + 'vital_signs_editors.db'); cursor = conn.cursor()

    for languagecode in wikilanguagecodes_plusmeta:
        print (languagecode)
        a_file = open(databases_path+"temporary_editor_metrics.txt")
        parameters = csv.reader(a_file, delimiter="\t", quotechar = '|')

        query = 'UPDATE '+languagecode+'wiki_editors SET (primarylang, primary_ecount, totallangs_ecount, numberlangs, primary_year_month_first_edit, primary_lustrum_first_edit) = (?,?,?,?,?,?) WHERE user_name = ?;'

        cursor.executemany(query,parameters)
        conn.commit()


    print ("All the original tables updated with the editors' primary language")

    try: os.remove(databases_path +'temporary_editor_metrics.txt')
    except: pass





#######################################################################################
  
class Logger_out(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("vital_signs.out", "w")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        pass
class Logger_err(object): # this prints both the output to a file and to the terminal screen.
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("vital_signs.err", "w")
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
    # print ('checking languages Replicas databases and deleting those without one...')
    # # Verify/Remove all languages without a replica database
    # for a in wikilanguagecodes:
    #     if wikilanguages_utils.establish_mysql_connection_read(a)==None:
    #         wikilanguagecodes.remove(a)
    # print (wikilanguagecodes)

    wikipedialanguage_numberarticles = []

#    wikilanguagecodes = ['ca']

    wikilanguagecodes_plusmeta = wikilanguagecodes
    wikilanguagecodes_plusmeta.append('meta')

#    wikilanguagecodes_plusmeta = ['meta']

    
    print ('* Starting the COMMUNITY HEALTH METRICS '+cycle_year_month+' at this exact time: ' + str(datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
    main()

    finishTime = time.time()
    print ('* Done with the COMMUNITY HEALTH METRICS completed successfuly after: ' + str(datetime.timedelta(seconds=finishTime - startTime)))
    wikilanguages_utils.finish_email(startTime,'vital_signs.out', 'COMMUNITY HEALTH METRICS')
