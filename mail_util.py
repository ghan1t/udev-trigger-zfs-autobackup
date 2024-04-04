import subprocess
from email.message import EmailMessage
from log_util import Logging
from config_reader import EmailConfig

# Enclose the mail sending logic in a function
def send_email(subject, body, config: EmailConfig, logger: Logging):

    # Create the plain-text email
    message = EmailMessage()
    message.set_content(body)  # Set email body
    message['Subject'] = subject  # Set email subject
    message['From'] = config.fromaddr  # Set email from
    message['To'] = config.recipients # All recipients

    # Send the email
    try:
        subprocess.run(["/usr/sbin/sendmail", "-t", "-i"], input=message.as_bytes(), check=True)
        logger.log(f"Email sent successfully to {config.recipients}!")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error sending email to {config.recipients}: {e}")

def mail(message: str, config: EmailConfig, logger: Logging):
    logger.log(message)
    if config is not None:
        send_email("ZFS-Autobackup with UDEV Trigger", message, config, logger)
    
def mail_error(message: str, config: EmailConfig, logger: Logging):
    logger.error(message)
    if config is not None:
        send_email("ERROR: ZFS-Autobackup with UDEV Trigger", message, config, logger)

def mail_exception(message: str, config: EmailConfig, logger: Logging):
    logger.exception(message)
    if config is not None:
        send_email("ERROR: ZFS-Autobackup with UDEV Trigger", message, config, logger)
