# Databricks notebook source
import smtplib
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders


def send_mail(send_from = "db@pella.com", send_to = "shakyas@pella.com", subject = "Notification Alert !", message = "Notification Alert !",isHTMLMsg=False, files=["/mnt/<Mounted Point Directory>/"],server="mail.pella.com", port=25,use_tls=True, isStatEngineRun=False):
    
    msg = MIMEMultipart()
    msg['From'] = send_from
    #msg['To'] = send_to
    msg['To'] = " ,".join(send_to)
    print('sendto')
    print(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    
    #File Handling-Mount Path
    if(files is not None):
        print('File Mount Paths Provided')
        for path in files:
            part = MIMEBase('application', "octet-stream")
            with open(path, 'rb') as file:
                part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                        'attachment; filename="{}"'.format(Path(path).name))
            msg.attach(part)
    
    #HTML Message Handling
    if isHTMLMsg==True:
        partHTML = MIMEText(message, 'html')
        msg.attach(partHTML)
    else:
        msg.attach(MIMEText(message))

    #Stat Engine Run- Custom Handling for .xls attachment
    if isStatEngineRun==True:
        partxlsx = MIMEBase('application', "octet-stream")
        partxlsx.set_payload(open("StatEngineAuditResults.xlsx", "rb").read())
        encoders.encode_base64(partxlsx)
        partxlsx.add_header('Content-Disposition', 'attachment; filename="StatEngineAuditResults.xlsx"')
        msg.attach(partxlsx)
        
    #print(msg)
    smtp = smtplib.SMTP(server, port)
    if use_tls:
        smtp.starttls()
    #smtp.login(username, password)
    #print(send_to)
    #send_to_list = send_to.split(",")
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()
