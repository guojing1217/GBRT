#!/usr/bin/env python
import pandas as pd
import sys
from sqlalchemy import create_engine
import datetime as dt
import types
import os

def main():
    disk_engine = create_engine('sqlite:///data.db')
    for csvfile in sys.argv[1:]:
        print "processing %s." % csvfile
        df_csv = pd.read_csv(csvfile,usecols=[0,1,2,3,4,5,6,7,8,9,10],names = ['jobid','jobtype','state','status','class','schedule','client','server','started','elapsed','ended'])
        df_csv = df_csv.dropna()
        df_csv = df_csv[df_csv['class'].str.contains('SLP') == False]
        if not os.path.exists('data.db'):
            print "Adding %d records" % df_csv.shape[0]
            df_csv.to_sql('data',disk_engine,if_exists='append',index=False,index_label='jobid')
            continue
        df_data = pd.read_sql_table('data',disk_engine)
        for jobid in df_csv['jobid']:
            if jobid in df_data.jobid.values:
                df_csv= df_csv[df_csv.jobid != jobid]
        print "Adding %d records" % df_csv.shape[0]
        df_csv.to_sql('data',disk_engine,if_exists='append',index=False,index_label='jobid')


if __name__ == '__main__':
    main()
