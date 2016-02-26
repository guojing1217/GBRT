#!/usr/bin/env python
import smtplib
import pyzmail
import imapclient
import pprint
import time
import os
import csv
import pandas as pd
import email,email.encoders,email.mime.text,email.mime.base
from glob import glob
import sqlite3
import logging
from logging.handlers import SMTPHandler



class rsmail:
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
        conn = sqlite3.connect(self.__db_name_conf)
        c = conn.cursor()
        c.execute("INSERT INTO EMAIL_FILE_LIST (email_from,email_to,email_uid,email_attachment,email_processed) values(?,?,?,?,?)",(email_from,username,uid,ori_filename_attachment,proccessflag))
        conn.commit()
        logger.info('1 record is loaded into EMIL_FROM_LIST table')

    def Imap_Connect_Email(self):
        self.__obj_imap= imapclient.IMAPClient(self.url_imap,ssl=True)
        self.__obj_imap._MAXLINE = 10000000
        self.__obj_imap.login(self.username_email,self.password_email)
        logger.info('Imap_Connect_Email')
        
        
    def Smtp_Connect_Email(self):
        self.__obj_smtp = smtplib.SMTP(self.url_email,self.port_smtp_email)
        self.__obj_smtp.ehlo()
        self.__obj_smtp.starttls()
        self.__obj_smtp.login(self.username_email,self.password_email)
        logger.info('Smtp_Connect_Email')


    def ProcessEmail(self,subjectstr='bpdbjobs',deleteflag=False,folder='INBOX'):
        
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
                logger.info('email %d index %d disposition %r' % (uid,index,mailpart.part.get("Content-Disposition", None)))
                content_disposition = mailpart.part.get("Content-Disposition", None)
                if content_disposition:
                    dispositions = content_disposition.strip().split(";")
                else:
                    continue
                if dispositions[0].lower() != "attachment":
                    continue                                
                
                if mailpart.type.startswith('text/'):
                    payload, used_charset=pyzmail.decode_text(mailpart.get_payload(), mailpart.charset, None)
                    filename = self.file_save_path + mailpart.filename + '_uid' + str(uid) + '_' + time.strftime('-%Y%m%d-%H%M%S') + ".txt"
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
        conn = sqlite3.connect(self.__db_name_conf)
        c = conn.cursor()
        tablist = c.execute("SELECT * from EMAIL_FILE_LIST")
        for id,reply_from,reply_username,reply_uid,file_attached,reply_process_status in tablist:
            if reply_process_status == 'Processed':
                continue
            html = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" '
            html +='"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"><html xmlns="http://www.w3.org/1999/xhtml">'
            html +='<body style="font-size:12px;font-family:Verdana"><p>...</p>'
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
                c.execute("UPDATE EMAIL_FILE_LIST SET email_processed='Proccessed' WHERE email_attachment=?",[file_attached])
            self.__obj_smtp.sendmail('gotham.the.penguin@gmail.com',reply_from,emailMsg.as_string())
            logger.info('Reply email has been sent')
        conn.commit()


if __name__ == '__main__':
    
    logging.basicConfig(filename='./James_Gordon.log',level=logging.INFO,filemode='w+',format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logger = logging.getLogger()
    logger.info('\n\n\n----------------------------------------------------------------')
    
    obj_rsmail = rsmail(url_email = 'smtp.gmail.com',\
            port_smtp_email = 587,\
            username_email = 'gotham.the.penguin@gmail.com',\
            password_email = 'james_gordon',\
            url_imap = 'imap.gmail.com')
    obj_rsmail.Imap_Connect_Email()
    obj_rsmail.ProcessEmail(subjectstr='bpdbjobs',deleteflag=False)
    #obj_rsmail.Smtp_Connect_Email()
    #obj_rsmail.ReplySenders()
