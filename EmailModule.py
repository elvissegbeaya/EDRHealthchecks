import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import ssl
import traceback
import sys


def send_error_email():
    smtp_server = 'smtp.office365.com'
    smtp_port = 587
    nov_user = 'MD-USA-ICDLogging@nov.com'
    nov_password = '$BjRx89cwl^mzgG'
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    error_msg = f"Error occured: {traceback_msg}"

    # constructing email
    sender_email = 'MD-USA-ICDLogging@nov.com'
    recipient_email = "elvis.segbeaya@nov.com"
    subject = "Error occured in code"
    body = error_msg
    message = f"Subject: {subject} \n\n{body}"

    # send email
    with smtplib.SMTP('smtp.office365.com', 587) as server:
        server.starttls()
        server.login(nov_user, nov_password)
        server.sendmail(sender_email, recipient_email, message)


def send_email(subject= 'EDR Report Coming Soon', body= "Your EDR report is on it's way", recipient= 'elvis.segbeaya@nov.com', attachment_path=None, **kwargs):
    # Set up the email message
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = 'MD-USA-ICDLogging@nov.com'
    msg['To'] = recipient

    # Add the body of the email as a plain text attachment
    body_attachment = MIMEText(body)
    msg.attach(body_attachment)

    # If an attachment path is provided, add the Excel file as an attachment
    if attachment_path:
        with open(attachment_path, 'rb') as f:
            excel_attachment = MIMEApplication(f.read(), _subtype='xlsx')
            excel_attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment_path))
            msg.attach(excel_attachment)

    # Send the email using SMTP
    smtp_server = 'smtp.office365.com'
    smtp_port = 587
    # smtp_username = 'MD-USA-ICDLogging@nov.com'
    # smtp_password = '$BjRx89cwl^mzgG'
    smtp_username = 'MD-USA-ICDLogging@nov.com'
    smtp_password = '$BjRx89cwl^mzgG'
    with smtplib.SMTP(smtp_server, smtp_port) as smtp:
        smtp.starttls()
        smtp.login(smtp_username, smtp_password)
        smtp.send_message(msg)


send_email()