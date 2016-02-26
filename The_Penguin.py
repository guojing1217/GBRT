import pyzmail
from time import *
import datetime

sender=(u'Cobblepot', 'gotham.the.penguin@gmail.com')
recipients=[(u'The Penguin', 'gotham.the.penguin@gmail.com'), 'gotham.the.penguin@gmail.com']
subject=u'Pilot'
text_content=u'text_content'
prefered_encoding='iso-8859-1'
text_encoding='iso-8859-1'

payload, mail_from, rcpt_to, msg_id=pyzmail.compose_mail(\
    sender, \
    recipients, \
    subject, \
    prefered_encoding, \
    (text_content, text_encoding), \
    html=None, \
    attachments=[('attached content L1\n L2\n L3\n L4\n L5\n L6\n', 'text', 'plain', 'attachment_txt.txt', \
    'us-ascii')])

print payload

print 'Sender address:', mail_from
print 'Recipients:', rcpt_to

smtp_host='smtp.gmail.com'
smtp_port=587
smtp_mode='tls'
smtp_login='gotham.the.penguin@gmail.com'
smtp_password='james_gordon'

'''
ret=pyzmail.send_mail(payload, mail_from, rcpt_to, smtp_host, \
                smtp_port=smtp_port, smtp_mode=smtp_mode, \
                        smtp_login=smtp_login, smtp_password=smtp_password)

if isinstance(ret, dict):
        if ret:
            print 'failed recipients:', ', '.join(ret.keys())
        else:
            print 'success'
else:
    print 'error:', ret
'''


msg=pyzmail.PyzMessage.factory(payload)

print 'Subject: %r' % (msg.get_subject(), )
print 'From: %r' % (msg.get_address('from'), )
print 'To: %r' % (msg.get_addresses('to'), )
print 'Cc: %r' % (msg.get_addresses('cc'), )


for mailpart in msg.mailparts:
        print '    %sfilename=%r alt_filename=%r type=%s charset=%s desc=%s size=%d' % ( \
                '*'if mailpart.is_body else ' ', \
                mailpart.filename,  \
                mailpart.sanitized_filename, \
                mailpart.type, \
                mailpart.charset, \
                mailpart.part.get('Content-Description'), \
                len(mailpart.get_payload()) )
        if mailpart.type.startswith('text/'):
            payload, used_charset=pyzmail.decode_text(mailpart.get_payload(), mailpart.charset, None)
            print '        >', payload
            print 'Current time is : %s' % ctime()
