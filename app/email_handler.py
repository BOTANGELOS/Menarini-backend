
import imaplib
import email
from email.header import decode_header
from .file_parser import save_attachment
import re

def connect_to_email(username, password, imap_server):
    print('***************')
    print(username, password, imap_server)
    mail = imaplib.IMAP4_SSL(imap_server)
    mail.login(username, password)
    return mail

def fetch_emails(mail):
    mail.select("inbox")
    status, messages = mail.search(None, "ALL")
    email_ids = messages[0].split()
    emails = []
    for e_id in email_ids:
        _, msg = mail.fetch(e_id, "(RFC822)")
        emails.append(email.message_from_bytes(msg[0][1]))
    return emails

def decode_mime_header(header_val):
    if header_val is None:
        return ""
    decoded, charset = decode_header(header_val)[0]
    if isinstance(decoded, bytes):
        return decoded.decode(charset or 'utf-8', errors='ignore')
    return decoded
def extract_email_content(email_msg):
    subject = decode_mime_header(email_msg["Subject"])
    sender = decode_mime_header(email_msg["From"])
    body = ""
    attachments = []

    if email_msg.is_multipart():
        for part in email_msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                body += part.get_payload(decode=True).decode()
            elif part.get("Content-Disposition"):
                filename = part.get_filename()
                if filename:
                    filename = decode_header(filename)[0][0]
                    if isinstance(filename, bytes):
                        filename = filename.decode(errors='ignore')

                    # Remove any hidden newline characters (\r, \n)
                    filename = filename.replace("\r", "").replace("\n", "")

                    # Sanitize the filename by replacing invalid characters
                    filename = re.sub(r'[\/:*?"<>|]+', '_', filename)

                else:
                    return None  # If there's no filename, return None

                filepath = filename
                print(filepath)
                attachments.append(filepath)
    else:
        body = email_msg.get_payload(decode=True).decode()
    return subject, sender, body, attachments
