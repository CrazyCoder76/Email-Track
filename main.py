import json
import os.path
import base64
import email
import re
from time import sleep
from typing import Any, Dict, List, Optional
import uuid
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import yaml

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]

def get_service() -> build:
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    service = build("gmail", "v1", credentials=creds)
    return service

def list_messages(service: build, user_id: str) -> List[Dict]:
    try:
        response = service.users().messages().list(userId=user_id, q="is:unread").execute()
        messages = response.get("messages", [])
        return messages
    except Exception as error:
        print(f"An error occurred: {error}")

def mark_message_as_read(service: build, user_id: str, msg_id: str) -> None:
    try:
        service.users().messages().modify(userId=user_id, id=msg_id, body={"removeLabelIds": ["UNREAD"]}).execute()
    except Exception as error:
        print(f"An error occurred: {error}")

def extract_email_address(data: str) -> Optional[str]:
    match = re.search(r'[\w\.-]+@[\w\.-]+', data)
    if match:
        return match.group(0)
    else:
        return None

def get_message(service: build, user_id: str, msg_id: str, store_dir: str, configuration: Dict[str, Any]) -> None:
    try:
        message = service.users().messages().get(userId=user_id, id=msg_id, format="full").execute()
        
        headers = message['payload']['headers']
        
        from_email = extract_email_address(next(header['value'] for header in headers if header['name'] == 'From'))
        to_email = next(header['value'] for header in headers if header['name'] == 'To')
        date = next(header['value'] for header in headers if header['name'] == 'Date')

        if configuration['email'] != to_email:
            return
        
        if from_email not in configuration['allowed_senders']:
            return
        
        print(f"From : {from_email}")
        print(f"To : {to_email}")
        print(f"Date : {date}")

        # Check for attachments
        for part in message["payload"].get("parts", ""):
            file_name = part["filename"]
            body = part["body"]
            if "attachmentId" in body:
                attachment_id = body["attachmentId"]
                attachment = service.users().messages().attachments().get(userId=user_id, messageId=msg_id, id=attachment_id).execute()
                
                data = attachment["data"]
                file_data = base64.urlsafe_b64decode(data.encode("UTF-8"))
                file_path = str(uuid.uuid4()).upper()

                path = os.path.join(store_dir, f"{file_path}.csv")
                
                with open(path, "wb") as f:
                    f.write(file_data)

                print(f"Attachment {file_name} saved to {path}")

                path = os.path.join(store_dir, f".{file_path}.meta")
                meta_data = {
                    "sender_email": from_email,
                    "receiver_email": to_email,
                    "repository": configuration['repo'],
                    "table": configuration['table'],
                    "timestamp": date,
                    "file_format": "csv",
                    "attachment_filename": file_name
                }
                with open(path, "w") as f:
                    json.dump(meta_data, f)
                    
                print(f"Meta file {file_name} saved to {path}")


    except Exception as error:
        print(f"An error occurred: {error}")

def main():
    service = get_service()
    user_id = "me"
    store_dir = "download_dir"

    stream = open("configuration.yaml", 'r')
    configuration = yaml.load(stream , Loader=yaml.FullLoader)

    if not os.path.exists(store_dir):
        os.makedirs(store_dir)

    while True:
        print("Checking for new emails...")
        messages = list_messages(service, user_id)
        
        if not messages:
            print("No messages found.")
        else:
            print("Message IDs:")
            for message in messages:
                print(message["id"])
                get_message(service, user_id, message["id"], store_dir, configuration)

                mark_message_as_read(service, user_id, message["id"])
        sleep(60)

if __name__ == "__main__":
    main()