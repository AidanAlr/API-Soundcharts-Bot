import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from src.credentials_key_info import SENDER_EMAIL, SMTP_KEY
from src.logging_config import logger


def send_email(recipient: str, subject: str, message: str, attachment_paths: list):
    sender_email = SENDER_EMAIL
    smtp_key = SMTP_KEY
    recipient_email = recipient

    # Setup the email message
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = recipient_email
    msg["Subject"] = subject
    msg.attach(MIMEText(message, "plain"))

    # Attach all files from the list of attachment paths
    for attachment_path in attachment_paths:
        try:
            with open(attachment_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {os.path.basename(attachment_path)}",
            )
            msg.attach(part)
        except Exception as e:
            logger.warning(f"Failed to attach file {attachment_path}: {e}")

    # Connect to the SMTP server
    try:
        server = smtplib.SMTP("smtp-relay.brevo.com", 587)
        server.starttls()
        server.login(sender_email, smtp_key)
    except Exception as e:
        logger.debug(f"Failed to connect to SMTP server: {e}")
        return False

    # Send the email
    try:
        server.sendmail(sender_email, recipient_email, msg.as_string())
        logger.info(f"Email notification sent successfully to: {recipient_email}")
        return True
    except Exception as e:
        logger.debug(f"Failed to send email: {e}")
        return False
    finally:
        server.quit()
