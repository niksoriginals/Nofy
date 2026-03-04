import os
import json
import threading
import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone

if "FIREBASE_SERVICE_ACCOUNT" not in os.environ:
    raise ValueError("❌ FIREBASE_SERVICE_ACCOUNT environment variable not found!")
service_account_json = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
if "private_key" in service_account_json:
    service_account_json["private_key"] = service_account_json["private_key"].replace("\\n", "\n")

if not firebase_admin._apps:
    cred = credentials.Certificate(service_account_json)
    firebase_admin.initialize_app(cred)

db = firestore.client()
SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]
credentials_fc = service_account.Credentials.from_service_account_info(
    service_account_json, scopes=SCOPES
)
project_id = service_account_json["project_id"]

def get_access_token():
    """FCM ke liye fresh access token generate karna."""
    global credentials_fc
    if not credentials_fc.valid or credentials_fc.expired:
        credentials_fc.refresh(Request())
    return credentials_fc.token
def send_fcm_notification(collection_name, doc_id, title):
    try:
        access_token = get_access_token()
        url = f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
        
        message = {
            "message": {
                "topic": "allUsers",
                "notification": {
                    "title": "📢 Campus Update",
                    "body": title
                },
                "android": {
                    "priority": "HIGH",
                    "notification": {
                        "channel_id": "high_importance_channel",
                        "sound": "default",
                    }
                },
                "data": {
                    "collection": collection_name,
                    "doc_id": doc_id,
                }
            }
        }
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        resp = requests.post(url, headers=headers, json=message)
        if resp.status_code == 200:
            print(f"✅ Notification Sent: {title}")
        else:
            print(f"❌ FCM Error: {resp.text}")
    except Exception as e:
        print(f"⚠️ FCM Function Error: {e}")
def on_snapshot(col_name):
    def callback(col_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == 'ADDED':
                data = change.document.to_dict()
                doc_id = change.document.id
                doc_ts = data.get("timestamp")
                if doc_ts:
                    if hasattr(doc_ts, "to_datetime"):
                        doc_dt = doc_ts.to_datetime().replace(tzinfo=timezone.utc)
                    else:
                        doc_dt = doc_ts
                    time_diff = (datetime.now(timezone.utc) - doc_dt).total_seconds()
                    
                    if time_diff < 15:
                        title = data.get("title", "New Post")
                        print(f"🔔 New Document in {col_name}: {title}")
                        send_fcm_notification(col_name, doc_id, title)

    return callback
print("🚀 Live Listener Started... Monitoring for changes.")

collections = ["news", "events", "files"]
watchers = []

for col in collections:
    query_watch = db.collection(col).on_snapshot(on_snapshot(col))
    watchers.append(query_watch)
stop_event = threading.Event()
stop_event.wait()
