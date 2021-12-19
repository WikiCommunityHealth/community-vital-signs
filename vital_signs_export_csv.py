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
from random import shuffle
# data
import pandas as pd
import gc



databases_path = '/mnt/backdata/databases/'


#### ARTICLES DATA ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### 



### --------------------------------------------------------------------------------


wikilanguages = ['ca','eu']
wikilanguages = ['is']
wikilanguages = ['gl','eu','gl']

wikilanguages = ['ca','it']
wikilanguages = ['it']

wikilanguages = ['gl','ca','eu']
wikilanguages = ['ca','es','eu','fr','it']


#wikilanguages = ['ary']




wikilanguages = ['ar','arz','ary','it','fr','de','nl','meta']



# wikilanguages = ['ca','es','eu','fr','it','pt','ro','ar','de','gl','meta'] # catalan and european




wikilanguages = ['av', 'be_x_old', 'ce', 'crh', 'cu', 'cv', 'ru', 'tt', 'lt', 'koi', 'tyv', 'rue', 'csb', 'pl', 'ro', 'uk', 'kv', 'bxr', 'lbe', 'bg', 'krc', 'inh', 'ba', 'olo', 'os', 'rmy', 'sl', 'udm', 'kbd', 'mrj', 'vep', 'gag', 'be', 'cs', 'sah', 'yi', 'de', 'mdf', 'mk', 'mn', 'myv', 'lez', 'tr', 'hu', 'mhr', 'ady', 'xal', 'szl', 'sk', 'meta']



wikilanguages = ['nso', 'kr', 'rn', 'ff', 'tum', 'arz', 'pt', 'ki', 'ti', 'sw', 'kbp', 'ha', 'aa', 'ln', 'wo', 'nqo', 'es', 'ig', 'ee', 'fr', 'ak', 'hz', 'af', 'ng', 'sn', 'sg', 'st', 'yo', 'bm', 'ss', 'din', 'ts', 'mg', 'kj', 'lg', 'am', 'kg', 'simple', 'it', 'om', 'so', 'tw', 've', 'tn', 'zu', 'ary', 'ar', 'rw', 'xh', 'ny', 'kab', 'meta']

wikilanguages = ['it', 'bar', 'co', 'de', 'eml', 'fr', 'frp', 'fur', 'lij', 'lld', 'lmo', 'nap', 'oc', 'pms', 'roa_tara', 'sc', 'scn', 'sl', 'sq', 'vec', 'ca', 'fr', 'ro', 'hr', 'el', 'lad', 'es', 'rm', 'meta']


#wikilanguages = ['it']


i = 0
for languagecode in wikilanguages:
    print (languagecode)

    conn = sqlite3.connect(databases_path + 'vital_signs_editors.db'); cursor = conn.cursor() # stats_prova_amb_indexs


    current_base = 0
    upper_threshold = 0

    cursor.execute('SELECT MAX(user_id) FROM '+languagecode+'wiki_editors;')
    maxuser_id = cursor.fetchone()[0]

    print (maxuser_id)

    while upper_threshold < maxuser_id:

        upper_threshold = current_base + 200000


        # EDITORS CHARACTERISTICS AND METRICS (ACCUMULATED)
        metrics = ["edit_count_24h", "edit_count_30d", "edit_count_60d", "edit_count_7d"] 
        print (len(metrics))


#        metrics = ["edit_count_bin","monthly_edit_count_bin","inactivity_periods","active_months","total_months","max_active_months_row","max_inactive_months_row","months_since_last_edit","over_edit_bin_average_past_max_inactive_months_row","over_monthly_edit_bin_average_past_max_inactive_months_row","over_past_max_inactive_months_row"]



        query = 'SELECT user_name, metric_name, abs_value, year_month FROM '+languagecode+'wiki_editor_metrics WHERE metric_name IN ('+','.join( ['?'] * len(metrics) )+')'

        query += ' AND user_id BETWEEN '+str(current_base)+' AND '+str(upper_threshold)+';'

        df = pd.read_sql_query(query, conn, params = metrics)


        # df.loc[~df.duplicated()].to_csv('mec.csv')
        # input('')

        # print ('here')
        # print (len(df))
        # print (len(df.drop_duplicates(keep="first")))
        # df = df.drop_duplicates(keep="first")


        # print (df[df.duplicated()].head(1000))
        # df = df[df.duplicated()]

        # input('')

        # print (df[df.duplicated('metric_name')].head(100))
#        print (df.loc[df.duplicated()])


        df = df.loc[~df.duplicated()]

        df1 = df.pivot(index='user_name', columns='metric_name', values = ['abs_value'])
        
        # print ('beggining')
        print (query)
        # print (len(df1.columns.tolist()))
        # print (df1.columns.tolist())

        cols = []
        for v in df1.columns.tolist(): cols.append(v[1]) 
        df1.columns = cols


        for m in metrics:
            if m not in df1:
                df1[m]=''

        df1 = df1.reindex(sorted(df1.columns), axis=1)


        # print (df1.head(10))
        # print (df1.columns.tolist())


        query = 'SELECT user_id, user_name, bot, user_flags, highest_flag, highest_flag_year_month, gender, primarybinary, primarylang, edit_count, primary_ecount, totallangs_ecount, primary_lustrum_first_edit, numberlangs, registration_date, year_month_registration, first_edit_timestamp, year_month_first_edit, year_first_edit, lustrum_first_edit, survived60d, last_edit_timestamp, year_last_edit, lifetime_days, days_since_last_edit FROM '+languagecode+'wiki_editors'


        query += ' WHERE user_id BETWEEN '+str(current_base)+' AND '+str(upper_threshold)+';'


        df2 = pd.read_sql_query(query, conn)
        df2 = df2.set_index('user_name')
        df2['language'] = languagecode

        # if i == 0:
        #     df2.to_csv(databases_path + 'langwiki_editors_italian_languages.tsv')
        # else:
        #     df2.to_csv(databases_path + 'langwiki_editors_italian_languages.tsv', mode='a', header=False)


        df3 = pd.concat([df1, df2], axis=1, sort=False)
        df3 = df3.reset_index().rename(columns = {'index':'user_name'}).set_index('user_name')
        df3['language'] = languagecode

#        df3 = df3.fillna(0)


        print (query)
        # print (len(df3.columns))
        # print (df3.head(10))

        df3['language-user_name'] = df3['language']+'-'+df3.index.astype(str)
       

        if i == 0:
            df3.to_csv(databases_path + 'langwiki_editors_characteristics_metrics_accumulated_italian_languages.tsv', sep='\t')
        else:
            df3.to_csv(databases_path + 'langwiki_editors_characteristics_metrics_accumulated_italian_languages.tsv', mode='a', header=False, sep='\t')

        print (len(df3))

        i += 1
        print (current_base)
        print ('end')
        current_base = upper_threshold

    print ('end lang: '+languagecode)
#    print  (df3.head(10))
print ('end1')


"""

    ### --------------------------------------------------------------------------------


i = 0
for languagecode in wikilanguages:
    print (languagecode)

    conn = sqlite3.connect(databases_path + 'vital_signs_editors.db'); cursor = conn.cursor() # 

    current_base = 0
    upper_threshold = 0

    cursor.execute('SELECT MAX(user_id) FROM '+languagecode+'wiki_editors;')
    maxuser_id = cursor.fetchone()[0]

    print (maxuser_id)

    while upper_threshold < maxuser_id:

        upper_threshold = current_base + 200000

        # EDITORS CHARACTERISTICS AND METRICS (OVER TIME)
        metrics = ["monthly_edits", "monthly_edits_technical", "active_months_row", "monthly_edits_coordination"]


#        metrics = ["monthly_created_articles","monthly_deleted_articles","monthly_moved_articles","monthly_undeleted_articles","monthly_accounts_created","monthly_users_renamed","monthly_autoblocks","monthly_edits_reverted","monthly_reverts_made","monthly_editing_days","monthly_edits","monthly_edits_to_baseline","monthly_editing_days_to_baseline","monthly_edits_increasing_decreasing","month_since_last_edit","active_months_row","inactive_months_row"]


        query = 'SELECT user_name, user_id, year_month, metric_name, abs_value FROM '+languagecode+'wiki_editor_metrics WHERE metric_name IN ('+','.join( ['?'] * len(metrics) )+')'

        query += ' AND user_id BETWEEN '+str(current_base)+' AND '+str(upper_threshold)+';'


        # print (query)
        # input('')

        print (current_base, upper_threshold)

        df = pd.read_sql_query(query, conn, params = metrics)


        df1 = pd.pivot_table(df, index=['year_month','user_name'], columns='metric_name', values = 'abs_value')

        df1 = df1.reset_index()
        df1 = df1.set_index('user_name')



        for m in metrics:
            if m not in df1:
                df1[m]=''


        df1 = df1.reindex(sorted(df1.columns), axis=1)
        df1 = df1.fillna(0)

        df1['language'] = languagecode
        df1['language-user_name'] = df1['language']+'-'+df1.index.astype(str)


        if i == 0:
            df1.to_csv(databases_path + 'langwiki_editors_metrics_over_time_italian_languages_languages.tsv', sep='\t')
        else:
            df1.to_csv(databases_path + 'langwiki_editors_metrics_over_time_italian_languages_languages.tsv', mode='a', header=False, sep='\t')


        i += 1
        current_base = upper_threshold


    print ('end lang: '+languagecode)
    # input('')

print ('end2')

"""
