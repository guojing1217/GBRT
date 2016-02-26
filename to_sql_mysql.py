#!/usr/bin/env python
import pandas as pd
import sys
from sqlalchemy import create_engine
import datetime as dt
import types
import os
import mysql
import dateutil
import time
import numpy as np

def main():
    disk_engine = create_engine('mysql://Quantico:Monday123@quantico.chgivxnnhpn3.us-west-2.rds.amazonaws.com/Quantico')
    table_exist = False


    for csvfile in sys.argv[1:]:
        if 'bpdbjobs' in csvfile.lower():
            df_data = pd.read_sql_table('data',disk_engine)
            if table_exist or df_data.empty == False:
                print "Adding inc data"
                print "processing %s." % csvfile
                df_csv = pd.read_csv(csvfile,usecols=[0,1,2,3,4,5,6,7,8,9,10],names = ['jobid','jobtype','state','status','class','schedule','client','server','started','elapsed','ended'])
                df_csv = df_csv.dropna()
                df_csv = df_csv[df_csv['class'].str.contains('SLP') == False]
            
                df_data = pd.read_sql_table('data',disk_engine)

                for jobid in df_csv['jobid']:
                    if jobid in df_data.jobid.values:
                        df_csv= df_csv[df_csv.jobid != jobid]
                print "Adding %d records" % df_csv.shape[0]
                df_csv.to_sql('data',disk_engine,if_exists='append',index=False,index_label='jobid')
                if df_csv.shape[0] > 0:
                    table_exist = True
            else:
                print "Adding everything"
                print "processing %s." % csvfile
                df_csv = pd.read_csv(csvfile,usecols=[0,1,2,3,4,5,6,7,8,9,10],names = ['jobid','jobtype','state','status','class','schedule','client','server','started','elapsed','ended'])
                df_csv = df_csv.dropna()
                df_csv = df_csv[df_csv['class'].str.contains('SLP') == False]
                print "Adding %d records" % df_csv.shape[0]
                df_csv.to_sql('data',disk_engine,if_exists='append',index=False,index_label='jobid')
                if df_csv.shape[0] > 0:
                    table_exist = True
        elif 'opscenter' in csvfile.lower():

            df_data = pd.read_sql_table('data',disk_engine)

            #print "opscenter file %s" % csvfile

            unixdateparse = lambda x: dateutil.parser.parse(x)

            #df_csv_opscenter = pd.read_csv(csvfile,usecols=[0,1,2,3,4,5,6,7,8],names = ['client','class','schedule','state','status','jobtype','started','ended','server'],skiprows = 4,skip_blank_lines=True,parse_dates=['started','ended'],date_parser=unixdateparse,skipfooter=1)
            df_csv_opscenter = pd.read_csv(csvfile,usecols=[0,1,2,4,6,7],names = ['client','class','schedule','status','started','ended'],skiprows = 4,skip_blank_lines=True,parse_dates=['started','ended'],date_parser=unixdateparse,skipfooter=1)
            if df_data.empty == False:
                del df_data['jobid']
                del df_data['jobtype']
                del df_data['state']
                del df_data['server']
                del df_data['elapsed']

            print 'there are %d rows in %s' % (len(df_csv_opscenter),csvfile)
            
            df_csv_opscenter.started = df_csv_opscenter.started.apply(lambda d : time.mktime( d.timetuple() ) )
            df_csv_opscenter.ended = df_csv_opscenter.ended.apply(lambda d : time.mktime( d.timetuple() ) )
            
            jobtype = { 'MS-Windows':9,'Standard':0,'Oracle':4,'MS-Exchange-Server':16,'VMware':40,'-':99}
            state = {'Failed': 6,'Queued':0,'Active':1,'Done':3,'Suspended':4,'Incomplete':5,'Successful':6,'Partially Successful':7}
            #df_csv_opscenter.jobtype = df_csv_opscenter.jobtype.apply(lambda t: jobtype[t])
            #df_csv_opscenter.state = df_csv_opscenter.state.apply(lambda s: state[s])
            
            #print 'there are %d rows in %s' % (len(df_csv_opscenter),'df_csv_opscenter')
            #print 'there are %d rows in %s' % (len(df_data),'df_data')
            
            df_csv_opscenter['in_df_csv_opscenter'] = 'yes'
            df_data['in_df_data'] = 'yes'
            #df_diff = (df_csv_opscenter != df_data).any()
            df_merge = df_csv_opscenter.merge(df_data,on=['client','class','schedule','status','started','ended'],how='left')
            df_diff = df_merge[ (df_merge['in_df_csv_opscenter']=='yes') & (df_merge['in_df_data'].isnull())]
            del df_diff['in_df_data']
            del df_diff['in_df_csv_opscenter']
            df_diff = df_diff
            #df_diff = pd.DataFrame(df_csv_opscenter.values-df_data.values, columns=df_csv_opscenter.columns)
            if df_diff.empty == False:
                df_diff['jobid'] = df_diff.index
                print 'Adding %d new records from %s' % (len(df_diff.index),csvfile)
                df_diff.to_sql('data',disk_engine,if_exists='append',index=False)
            else:
                print 'Adding 0 new records from %s' % csvfile

if __name__ == '__main__':
    main()