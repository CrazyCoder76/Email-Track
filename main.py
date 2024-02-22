import imaplib
import email
from email.header import decode_header
import os
import re
import sys
import yaml
from time import sleep
import uuid
from typing import List, Optional, Dict, Any
from exception import YamlException, EmailException, DownloadException, FatalException

configuration: Dict[str, Any] = None
EMAIL_ACCOUNTS: List[str] = None
EMAIL_PASSWORDS: List[str] = None
STORE_DIR: str = None
IMAP_SERVER: str = None
IMAP_PORT: int = None

def load_yaml() -> None:
    global EMAIL_ACCOUNTS, EMAIL_PASSWORDS, STORE_DIR, IMAP_SERVER, IMAP_PORT, configuration
    try:
        with open("configuration.yaml", 'r') as stream:
            configuration = yaml.load(stream, Loader=yaml.FullLoader)
        
        EMAIL_ACCOUNTS = configuration['tracker_emails']
        EMAIL_PASSWORDS = configuration['passwords']
        STORE_DIR = configuration['download']
        IMAP_SERVER = configuration['imap_server']
        IMAP_PORT = configuration['imap_port']
    except Exception as e:
        raise YamlException(err_msg = "Failed to load configuration.yaml", excp=e)


def connect_to_email_server(
    email: str,
    password: str
)-> imaplib.IMAP4_SSL:
    try:
        mail: imaplib.IMAP4_SSL = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        mail.login(email, password)
        return mail
    except imaplib.IMAP4.error as e:
        raise EmailException(err_msg=f"Failed to connect {IMAP_SERVER}:{IMAP_PORT}.", excp=e)

def search_unread_emails(mail: imaplib.IMAP4_SSL) -> List[bytes]:
    try:
        mail.select('inbox')
        status, message_numbers = mail.search(None, 'UNSEEN')
        if status == 'OK':
            return message_numbers[0].split()
        return []
    except Exception as e:
        raise EmailException(err_msg="Failed to read inbox emails.", excp=e)


def extract_email_address(data: str) -> Optional[str]:
    match = re.search(r'[\w\.-]+@[\w\.-]+', data)
    return match.group(0) if match else None

def download_attachment(
    mail: imaplib.IMAP4_SSL,
    message_id: str,
    configuration: Dict[str, Any]
) -> None:
    try:
        if not os.path.exists(STORE_DIR):
            os.makedirs(STORE_DIR)
        
        status, data = mail.fetch(message_id, '(RFC822)')
        if status == 'OK':
            email_message = email.message_from_bytes(data[0][1])

            from_email = extract_email_address(email.utils.parseaddr(email_message['From'])[1])
            to_email = email.utils.parseaddr(email_message['To'])[1]
            date = email_message['Date']

            if to_email not in EMAIL_ACCOUNTS:
                return
            if from_email not in configuration['allowed_senders']:
                return

            for part in email_message.walk():
                if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
                    continue

                file_name: str = part.get_filename()
                if bool(file_name):
                    file_data = part.get_payload(decode=True)
                    save_file_name: str = str(uuid.uuid4()).upper()
                    _, ext = os.path.splitext(file_name)
                    file_extension: str = ext[1:]
                    file_path: str = os.path.join(STORE_DIR, f"{save_file_name}.{file_extension}")

                    with open(file_path, 'wb') as file:
                        file.write(file_data)

                    meta_data_path = os.path.join(STORE_DIR, f"{save_file_name}.meta")
                    meta_data = {
                        "sender_email": from_email,
                        "receiver_email": to_email,
                        "repository": configuration['repo'],
                        "table": configuration['table'],
                        "timestamp": date,
                        "file_format": file_extension,
                        "attachment_filename": file_name
                    }
                    with open(meta_data_path, "w") as meta_file:
                        yaml.dump(meta_data, meta_file)
    except Exception as e:
        raise DownloadException(err_msg=f"Failed to download attachment from message_id:{message_id} email.", excp=e)


def mark_as_read(
    mail: imaplib.IMAP4_SSL,
    message_id: str
) -> None:
    try:
        mail.store(message_id, '+FLAGS', '\Seen')
    except Exception as e:
        raise EmailException(err_msg="Failed to mark unread emails.", excp=e)

def main() -> None:
    try:
        load_yaml()
        while True:
            for i in range(len(EMAIL_ACCOUNTS)):
                try:
                    mail: imaplib.IMAP4_SSL = connect_to_email_server(EMAIL_ACCOUNTS[i], EMAIL_PASSWORDS[i])
                    unread_messages: List[bytes] = search_unread_emails(mail)

                    if unread_messages:
                        for message in unread_messages:
                            download_attachment(mail, message, configuration)
                            mark_as_read(mail, message)

                    mail.logout()
                except EmailException:
                    continue
                except DownloadException:
                    continue
                except Exception as e:
                    raise FatalException(excp=e)

            sleep(60)
    except YamlException:
        return
    except FatalException:
        return

if __name__ == "__main__":
    main()
