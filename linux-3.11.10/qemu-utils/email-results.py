#! /usr/bin/python

import subprocess
import sys
import getopt
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.mime.text import MIMEText

MAX_ATTACHMENT_LINES = 50000

gmail_user = "ftfs.sb@gmail.com"
gmail_pwd = "ftfs2014ABC"
FROM = 'ftfs.sb@gmail.com'


def tail(fname, nlines) :
	return subprocess.check_output(["tail", "-n", str(nlines), fname])


def send_mail(subject, emails, attachments):
	msg = MIMEMultipart()
	msg['Subject']= subject
	msg['From']= FROM
	msg['To']= emails

        msg.preamble = 'Automated test output.'

	for f in attachments :
		attachment = MIMEText(tail(f, MAX_ATTACHMENT_LINES))
		attachment.add_header('Content_Disposition' , 'attachment', filename = f )
		msg.attach( attachment )


	try:
	    server = smtplib.SMTP("smtp.gmail.com", 587) #or port 465 doesn't seem to work!
	    server.ehlo()
	    server.starttls()
	    server.login(gmail_user, gmail_pwd)
	    server.sendmail(FROM, emails.split(","), msg.as_string())
	    server.close()
	    print 'successfully sent the mail'
	except:
	    print "failed to send mail"


def usage() :
	print "args:"
	print "\t--subject=<subject of the email>"
	print "\t--attach=<comma separated list of file attachments>"
	print "\t--addresses=<comma separated list of email addresses>"
	print "\t--help"


if __name__ == "__main__":
	
	try:
	 	opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "subject=", "addresses=", "attachments="])
	except getopt.GetoptError:
		usage()
		sys.exit(2)

	subject = "undef"
	emails = "undef"
	files = "undef"

	for opt, arg in opts :
		if opt in ("-h", "--help") :
			usage()
		elif opt == "--subject" :
			subject = arg
		elif opt == "--attachments" :
			files = arg
		elif opt == "--addresses" :
			emails = arg
	
	if subject == "undef" or emails == "undef" or files == "undef" :
		usage()
		sys.exit(1)

        send_mail(subject, emails, files.split(","));
