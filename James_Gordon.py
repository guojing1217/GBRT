#!/usr/bin/env python
import os
import smtplib
import pyzmail
import imapclient
import time
import csv
import pandas as pd
import email,email.encoders,email.mime.text,email.mime.base
from glob import glob
import sqlite3
import logging
from logging.handlers import SMTPHandler
from sqlalchemy import create_engine
import datetime as dt
import pymysql
import sys
import MySQLdb
#from MySQLdb import connector
import pprint
import calendar
import collections
import webbrowser
import copy

table = collections.OrderedDict()


class DBConnector(object):
    conn = None
    c = None
    def __init__(self):
        self._dbhost = 'quantico.chgivxnnhpn3.us-west-2.rds.amazonaws.com'
        self._dbuser = 'Quantico'
        self._dbpswd = 'Monday123'
        self._dbdb = 'Quantico'
        logger.debug('%s,%s,%s,%s' % (self._dbhost,self._dbuser,self._dbpswd,self._dbdb))
        try: 
            self.conn = pymysql.connect(host=self._dbhost,user=self._dbuser,passwd=self._dbpswd,db=self._dbdb)
        except:
            logger.error('Failed to connect to database')
            raise
        self.c = self.conn.cursor()
        logger.info('Connected to database')
        
        tableEmailExists = False
        self.c.execute('show tables like "email"')
        for row in self.c:
            tableEmailExists=True
            
        if tableEmailExists != True:
            self.c.execute('create table email(id int(6) auto_increment primary key,email_from varchar(30),email_to varchar(30),email_uid int(6),email_attachment varchar(100),email_processed varchar(30))')
            logger.info('Creating table email')
        
    def close(self):
        self.conn.close()
    def ExecuteSQL(self,sql='',tablename=''):
        return
           
class EmailDB(DBConnector):
    def __init__(self):
        DBConnector.__init__(self)
    def Insert(self,email_from,username,uid,ori_filename_attachment,proccessflag):
        return
    def Update(self,email_from,username,uid,ori_filename_attachment,proccessflag):
        return
    def UpdateProcessStatus(self,id,status='Not Processed'):
        try: 
            self.c.execute('update email set email_processed="%s" where id="%d"' % (status,id))
            self.conn.commit()
            logger.debug('Update email id %d with %s status successfully' % (id,status))
        except:
            logger.debug('Failed to update status for email id %d' % id)       

class DataDB(EmailDB):
    def __init__(self):
        EmailDB.__init__(self)
    
    def MoveAttachmentToDB(self,filepath='./attachment'):
        #Get a list of files under ./attachment
        #Extract required fields from csv and load into table
        #Rename processed files from .act to .bak
        #Update conf db

        logger.info('Start trying to load records from csv filies to databases')
        try:         
            disk_engine = create_engine('mysql://Quantico:Monday123@quantico.chgivxnnhpn3.us-west-2.rds.amazonaws.com/Quantico')
            logger.info('Successfully connected to databases')
        except: 
            logger.error('Failed to connect to databases')
            return

        tableDataExists = False
        self.c.execute('show tables like "data"')
        for row in self.c:
            tableDataExists=True
        
        self.c.execute('select id,email_attachment from email where email_processed="Not Proccessed"')
        emailrecords = list(self.c)
        for (id,csvfile) in emailrecords:
            if 'bpdbjobs' in csvfile: 
                logging.info('Process bpdbjobs output attachment file %s' % csvfile)
                df_csv = pd.read_csv(csvfile,usecols=[0,1,2,3,4,5,6,7,8,9,10],names = ['jobid','jobtype','state','status','class','schedule','client','server','started','elapsed','ended'])
                df_csv = df_csv.dropna()
                df_csv = df_csv[df_csv['class'].str.contains('SLP') == False]
                if tableDataExists:
                    logger.debug('Table data exists')
                    df_data = pd.read_sql_table('data',disk_engine)
                    for jobid in df_csv['jobid']:
                        if jobid in df_data.jobid.values:
                            df_csv= df_csv[df_csv.jobid != jobid]
                    if df_csv.empty:
                        self.UpdateProcessStatus(id,'Processed')
                        logger.info('All records in this file exist already')
                    else:
                        df_csv.to_sql('data',disk_engine,if_exists='append',index=False,index_label='jobid')
                        logger.info('Loaded %d records from file id=%d into table data' % (len(df_csv),id))
                        self.UpdateProcessStatus(id,'Processed')
                else:
                    if df_csv.empty !=True:
                        try:
                            df_csv.to_sql('data',disk_engine,if_exists='append',index=False,index_label='jobid')
                        except:
                            logger.error('Failed to load %d records from file id=%d into table data' %(len(df_csv),id))
                        logger.info('Loaded %d records from file id=%d into table data' % (len(df_csv),id))
                        logger.info('update email set email_processed="Processed" where id=%d' % id)
                        self.UpdateProcessStatus(id,'Processed')
            elif 'DAILY' in csvfile:
                logger.info('Moving OpsCenter report attchment file %s' % csvfile)
                   
    def InitializeReportDB(self):
        #Create data tables if they don't exist    
        return


class rsmail(EmailDB):
    '''
    rsmail: Read, Save and Delete(if specified) email
    '''
    __obj_smtp = None
    __obj_imap = None
    __file_save_path = './attachment/'
    __db_name_conf = 'gotham.db'

    def __init__(self,url_email = None,port_smtp_email = None,username_email = None,password_email = None,url_imap = None):
        self.url_email = url_email
        self.port_smtp_email = port_smtp_email
        self.username_email = username_email
        self.password_email = password_email
        self.url_imap = url_imap
        self.file_save_path = './attachement/'
        self.filenames_attachment =[]
        DBConnector.__init__(self)
        logger.debug('email: %s\n smtp port:%d\n username:%s\n password:%s\n,file_save_path:%s' % (self.url_email,self.port_smtp_email,self.username_email,self.password_email,self.file_save_path))

    @property
    def url_email(self):
        return  self._url_email

    @url_email.setter
    def url_email(self,val):
        self._url_email = str(val)

    @property
    def port_smtp_email(self):
        return self._port_smtp_email

    @port_smtp_email.setter
    def port_smtp_email(self,val):
        self._port_smtp_email = int(val)

    @property
    def username_email(self):
        return self._username_email

    @username_email.setter
    def username_email(self,val):
        self._username_email = str(val)

    @property
    def password_email(self):
        return self._password_email

    @password_email.setter
    def password_email(self,val):
        self._password_email = str(val)

    @property
    def url_imap(self):
        return self._url_imap

    @url_imap.setter
    def url_imap(self,val):
        self._url_imap = str(val)

    @property
    def file_save_path(self):
        return self._file_save_path

    @file_save_path.setter
    def file_save_path(self,val):
        self._file_save_path = str(val)
       

    def filelist_csv_save(self,email_from,username,uid,ori_filename_attachment,proccessflag):
        try:
            sql = 'INSERT INTO email (email_from,email_to,email_uid,email_attachment,email_processed) values("%s","%s","%d","%s","%s")' % (email_from,username,uid,ori_filename_attachment,proccessflag)
            logger.debug('executing %s',str(sql))
            self.c.execute(sql)
            self.conn.commit()
        except mysql.connector.Error as err:
            logger.error('Failed to insert 1 record into table email. Error is {}'.format(err))
            return
        logger.debug('1 record is loaded into table email')

    def Imap_Connect_Email(self):
        self.__obj_imap= imapclient.IMAPClient(self.url_imap,ssl=True)
        self.__obj_imap._MAXLINE = 10000000
        self.__obj_imap.login(self.username_email,self.password_email)
        logger.debug('Imap_Connect_Email')
        
        
    def Smtp_Connect_Email(self):
        self.__obj_smtp = smtplib.SMTP(self.url_email,self.port_smtp_email)
        self.__obj_smtp.ehlo()
        self.__obj_smtp.starttls()
        self.__obj_smtp.login(self.username_email,self.password_email)
        logger.info('Smtp_Connect_Email')


    def StoreEmail(self,subjectstr='bpdbjobs',deleteflag=False,folder='INBOX'):
        
        logger.info('deleteflag is %r' % deleteflag)
        
        if deleteflag == True:
            self.__obj_imap.select_folder(folder,readonly=False)
        else:
            self.__obj_imap.select_folder(folder,readonly=True)        

        uids = self.__obj_imap.search('SUBJECT %s' % subjectstr)
        messages = {}
        filenames_saved = []
        
        for uid in uids:
            rawMessages = self.__obj_imap.fetch([uid],['BODY[]', 'FLAGS'])
            messages[uid] = pyzmail.PyzMessage.factory(rawMessages[uid]['BODY[]'])
            
        if messages == {}:
            logger.info( 'No emails are found with %s' % subjectstr )
        else:
            logger.info( '%d emails found with %s' % (len(messages),subjectstr) )
            
        for uid,msg in messages.iteritems():
            logger.info('Processing email id %d with subbject: %s' % (uid,msg.get_subject()))
            from_email = msg.get_addresses('from')
            for index,mailpart in enumerate(msg.mailparts):
                logger.debug('email %d index %d disposition %r' % (uid,index,mailpart.part.get("Content-Disposition", None)))
                content_disposition = mailpart.part.get("Content-Disposition", None)
                if content_disposition:
                    dispositions = content_disposition.strip().split(";")
                else:
                    continue
                if dispositions[0].lower() != "attachment":
                    continue

                logger.debug('mailpart.type is %s',mailpart.type)
                             
                if mailpart.type.startswith('text/') or mailpart.type.startswith('application/octet-stream'):
                    payload, used_charset=pyzmail.decode_text(mailpart.get_payload(), mailpart.charset, None)
                    filename = self.file_save_path + mailpart.filename + '_uid' + str(uid) + '_' + time.strftime('-%Y%m%d-%H%M%S') + ".act"
                    if not os.path.exists(self.file_save_path):
                        os.makedirs(self.file_save_path)
                    open(filename,'w').write(mailpart.get_payload())
                    self.filelist_csv_save(from_email[0][1],self.username_email,uid,filename,'Not Proccessed')
                    logger.info("Saving attachment in email %d %s as " % (uid,filename))
            if deleteflag == True:
                logger.info( "Deleting mail with uid %d..." % uid )
                self.__obj_imap.delete_messages(uid)
                self.__obj_imap.expunge()
        return

    def ReplySenders(self):
        tablist = self.c.execute("SELECT * from email")
        for id,reply_from,reply_username,reply_uid,file_attached,reply_process_status in tablist:
            if reply_process_status == 'Processed':
                continue
            html = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" '
            html +='"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"><html xmlns="http://www.w3.org/1999/xhtml">'
            html +='<body style="font-size:8px;font-family:Verdana"><p>...</p>'
            html += "</body></html>"
            emailMsg = email.MIMEMultipart.MIMEMultipart('alternative')
            emailMsg['Subject'] = "This is the subject from reply email"
            emailMsg['From'] = reply_from
            emailMsg.attach(email.mime.text.MIMEText(html,'html'))
            reply_file_pattern = file_attached[:-4] + '*'

            for res_file in glob(reply_file_pattern):
                #if res_file == file_attached:
                #    continue
                fileMsg = email.mime.base.MIMEBase('application','csv')
                fileMsg.set_payload(file(res_file).read())
                email.encoders.encode_base64(fileMsg)
                fileMsg.add_header('Content-Disposition','attachment',filename=res_file)
                emailMsg.attach(fileMsg)
                logger.info( "PROCESSING..." + file_attached )
                self.c.execute("UPDATE email SET email_processed='Proccessed' WHERE email_attachment=?",[file_attached])
            self.__obj_smtp.sendmail('gotham.the.penguin@gmail.com',reply_from,emailMsg.as_string())
            logger.info('Reply email has been sent')
        self.conn.commit()

def InitializeMonthlyTable():
    disk_engine = create_engine('mysql://Quantico:Monday123@quantico.chgivxnnhpn3.us-west-2.rds.amazonaws.com/Quantico')
    df_data = pd.read_sql_table('data',disk_engine)
    allclients = sorted(list(pd.unique(df_data.client.ravel())))
    
    client_job_status_dict = {}
    for client in allclients:
        client_job_status_dict[client] = {'success':0,'failure':0,'partial':0}
    
    start = dt.datetime(2016,2,1,18,0,0)
    days = calendar.monthrange(2016,2)[1]

    #start = dt.datetime(2016,1,18,16,0,0)
    #days = 4
    
    global table
    #un_sorted_table = {}
    for day in range(1,days+1):
        table[start] = copy.deepcopy(client_job_status_dict)
        for client in allclients:
            s=time.mktime(start.timetuple())
            t=start + dt.timedelta(days=1)
            e=time.mktime(t.timetuple())
            if len(df_data.query('started > {0} and started < {1} and client == "{2}" and status == {3}'.format(s,e,client,0))) == 0:
               table[start][client]['success'] = 0
               table[start][client]['failure'] = 0
               table[start][client]['partial'] = 0
            else:
                table[start][client]['success'] = len(df_data[ (df_data['started']>s) & (df_data['started']<e) & (df_data['client']==client) & (df_data['status']==0) ].index)
                table[start][client]['failure'] = len(df_data[ (df_data['started']>s) & (df_data['started']<e) & (df_data['client']==client) & (df_data['status']>1) ].index)
                table[start][client]['partial'] = len(df_data[ (df_data['started']>s) & (df_data['started']<e) & (df_data['client']==client) & (df_data['status']==1) ].index)
            
        start += dt.timedelta(days=1)
    
     
def GetUniqueClients():
    global table
    t1_set = []
    #t1_set = {x for client in table.items()[1] if client not in x}
    for k_date,v_dict_allclients in table.items():
        for client in v_dict_allclients:
            if client not in t1_set:
                t1_set.append(client)
    return set(t1_set)
       
def ConvertTableToHTML():
    global table
    #pprint.pprint(table)
    
    html = ''
    html = '<table border="1" style="width:100%"><tr><th>'
    for k in table.keys():
        html += '</th><th>' + k.strftime('%d/%m')
    html +='</th></tr>'
    
    sorted_clients = sorted(GetUniqueClients())
    for client in sorted_clients:
        dis_client = client.split('.')[0].lower()
        html+='<tr><td>' + dis_client
        for k_date,v_dict_allclients in table.items():
            suc = v_dict_allclients[client]['success']
            par = v_dict_allclients[client]['partial']
            fai = v_dict_allclients[client]['failure']
            #dis = '<font size=2>' + 'S' + str(suc) + ' ' + 'P' + str(par) + ' ' + 'F' + str(fai) + '</font>'
            dis = ''
            if fai > 0:
                html += '</td><td bgcolor=red>' + dis + '</td>'
            elif par > 0:
                html += '</td><td bgcolor=yellow>' + dis + '</td>'
            elif suc > 0:
                html += '</td><td bgcolor=green>' + dis + '</td>'
            else:
                html += '</td><td>' + dis + '</td>'
        html+= '</tr>'
    html += '</table>'

    #pprint.pprint(table)      
    #pprint.pprint(html)    
    with open('table.html','w') as htmlfile:
        htmlfile.write(html)
    webbrowser.open_new('table.html')
    

if __name__ == '__main__':
    
    logging.basicConfig(filename='./James_Gordon.log',level=logging.DEBUG,filemode='w+',format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logger = logging.getLogger()
    logger.info('\n\n\n----------------------------------------------------------------')
    
    obj_rsmail = rsmail(url_email = 'smtp.gmail.com',\
            port_smtp_email = 587,\
            username_email = 'gotham.the.penguin@gmail.com',\
            password_email = 'james_gordon',\
            url_imap = 'imap.gmail.com')
    obj_rsmail.Imap_Connect_Email()
    obj_rsmail.StoreEmail(subjectstr='backup',deleteflag=True)
    #obj_rsmail.Smtp_Connect_Email()
    #obj_rsmail.ReplySenders()
    #DataDB().MoveAttachmentToDB()
    
    InitializeMonthlyTable()
    ConvertTableToHTML()
    
    
