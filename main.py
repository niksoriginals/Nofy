import os
import json
import time
import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone, timedelta

# 1. Firebase Initialization
if "FIREBASE_SERVICE_ACCOUNT" not in os.environ:
    raise ValueError("❌ FIREBASE_SERVICE_ACCOUNT environment variable not found!")

service_account_json = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
if "private_key" in service_account_json:
    service_account_json["private_key"] = service_account_json["private_key"].replace("\\n", "\n")

if not firebase_admin._apps:
    cred = credentials.Certificate(service_account_json)
    firebase_admin.initialize_app(cred)

db = firestore.client()

# 2. FCM Setup
SCOPES = ["https://www.googleapis.com/auth/firebase.messaging"]
credentials_fc = service_account.Credentials.from_service_account_info(
    service_account_json, scopes=SCOPES
)
project_id = service_account_json["project_id"]
LAST_FILE = "last_timestamp.txt"

def get_last_timestamp():
    """File se last timestamp uthao, agar nahi hai toh abhi se 1 minute pehle ka time do."""
    if os.path.exists(LAST_FILE):
        with open(LAST_FILE, "r") as f:
            ts_str = f.read().strip()
            if ts_str:
                return datetime.fromisoformat(ts_str)
    
    # Agar pehli baar chal raha hai toh abhi se 1 minute pehle ka time
    return datetime.now(timezone.utc) - timedelta(minutes=1)

def set_last_timestamp(ts: datetime):
    with open(LAST_FILE, "w") as f:
        f.write(ts.isoformat())

def check_firestore_and_send_notifications():
    global credentials_fc
    if not credentials_fc.valid or credentials_fc.expired:
        credentials_fc.refresh(Request())
    access_token = credentials_fc.token

    priority_order = ["news", "events", "files"]
    last_timestamp = get_last_timestamp()
    
    # DEBUG: Check kar rahe hain ki script kis time ke baad ka data dhoond raha hai
    print(f"🔍 Checking for updates after: {last_timestamp}")
    
    new_max_timestamp = last_timestamp

    for collection in priority_order:
        docs = (
            db.collection(collection)
            .where("timestamp", ">", last_timestamp)
            .order_by("timestamp", direction=firestore.Query.ASCENDING)
            .stream()
        )

        for doc in docs:
            data = doc.to_dict()
            doc_ts = data.get("timestamp")
            
            if not doc_ts: continue

            # Convert Firestore Timestamp to Python Datetime
            if hasattr(doc_ts, "to_datetime"):
                doc_dt = doc_ts.to_datetime().replace(tzinfo=timezone.utc)
            else:
                doc_dt = doc_ts

            print(f"🔔 New Entry Found: {collection} -> {data.get('title')}")

            # FCM Payload
            title = data.get("title", "New Update")
            message = {
                "message": {
                    "topic": "allUsers",
                    "notification": {"title": "📢 Campus Update", "body": title},
                    "android": {
                        "priority": "HIGH",
                        "notification": {
                            "channel_id": "high_importance_channel",
                            "sound": "default",
                        },
                    },
                    "data": {
                        "collection": collection,
                        "doc_id": doc.id,
                    },
                }
            }

            url = f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
            headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

            resp = requests.post(url, headers=headers, data=json.dumps(message))
            if resp.status_code == 200:
                print(f"✅ Notification Sent!")
            else:
                print(f"❌ FCM Error: {resp.text}")

            if doc_dt > new_max_timestamp:
                new_max_timestamp = doc_dt

    set_last_timestamp(new_max_timestamp)

# --- Loop ---
print("🚀 Watcher is running...")
while True:
    try:
        check_firestore_and_send_notifications()
    except Exception as e:
        print(f"⚠️ Error: {e}")
    time.sleep(60)
