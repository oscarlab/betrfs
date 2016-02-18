#! /usr/bin/python

import smtplib
from email.mime.text import MIMEText


gmail_user = "ftfs.sb@gmail.com"
gmail_pwd = "ftfs2014ABC"
FROM = 'ftfs.sb@gmail.com'
TO = 'yjiao@cs.stonybrook.edu'

textfile="ftfs-2014-05-23-18-20-54.out"
fp = open(textfile, 'rb')
msg = MIMEText(fp.read())
fp.close()

msg['Subject']= 'ftfs error log: %s' % textfile
msg['From']= FROM
msg['To']= TO

try:
    #server = smtplib.SMTP(SERVER) 
    server = smtplib.SMTP("smtp.gmail.com", 587) #or port 465 doesn't seem to work!
    server.ehlo()
    server.starttls()
    server.login(gmail_user, gmail_pwd)
    server.sendmail(FROM, [TO], msg.as_string())
    #server.quit()
    server.close()
    print 'successfully sent the mail'
except:
    print "failed to send mail"


