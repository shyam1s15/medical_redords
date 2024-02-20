from flask import Flask, request, jsonify
import flask
from sqlalchemy import Column, DateTime, Integer, String, create_engine, Date
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from datetime import datetime, timedelta
from api_response import APIResponse
import firebase_admin
from firebase_admin import credentials, auth
import json


db_params = {
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'database': os.environ.get('DB_NAME'),
}

# Check if the service account key JSON is available as an environment variable
if 'FIREBASE_SERVICE_ACCOUNT' in os.environ:
    # Load the service account key from the environment variable
    service_account_info = json.loads(os.environ['FIREBASE_SERVICE_ACCOUNT'])
    cred = credentials.Certificate(service_account_info)
else:
    # If the environment variable is not set, initialize Firebase Admin SDK without credentials
    cred = None

firebase_admin.initialize_app(cred)


# Construct the database URL
db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"

# Create the database engine
engine = create_engine(db_url, echo=True)

# Create a session factory
Session = sessionmaker(bind=engine)

Base = declarative_base()

def non_null_non_empty_list(collection) -> bool:
    return collection is not None and bool(collection)


def non_null_non_empty(data, key) -> bool:
    value = data.get(key)
    if value is None:
        return False
    
    if isinstance(value, list):
        return len(value) > 0
    else:
        return True
    
class Record(Base):
    __tablename__ = 'mo_records'
    id = Column(Integer, primary_key=True)
    opd_type = Column(Integer, name="opd_type")
    updated_at = Column(DateTime, default=datetime.utcnow, name="updated_at")
    opd_date = Column(Date, name="opd_date")
    firebase_user_id = Column(String, name="firebase_user_id")

class RecordGroup(Base):
    __tablename__ = 'record_groups'
    id = Column(Integer, primary_key=True)
    name = Column(String, name="name")
    new_male = Column(Integer, name="new_male")
    new_female = Column(Integer, name="new_female")
    old_male = Column(Integer, name="old_male")
    old_female = Column(Integer, name="old_female")
    record_id = Column(Integer, name="record_id")

def auth_user_by_token(request: flask.Request):
    user_token = request.headers.get("user-token")
    try:
        # Verify the Firebase ID token
        decoded_token = auth.verify_id_token(user_token)
        # Extract user ID from decoded token
        uid = decoded_token['uid']
        # Retrieve user information
        user = auth.get_user(uid)
        return user.uid
    except ValueError as e:
        # Handle invalid tokens or other errors
        print('Authentication failed:', e)
        return None

def export_medical_records(request: flask.Request) -> flask.typing.ResponseReturnValue:
    if request.method == 'OPTIONS':
    # Allows GET requests from any origin with the Content-Type
    # header and caches preflight response for an 3600s
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods":"*",
            "Access-Control-Allow-Headers":"*",
            "Access-Control-Allow-Credentials":"true",
            "Access-Control-Max-Age":"3600"
        }

        return ('', 200, headers)
    
    # user_id = auth_user_by_token(request=request)
    # if user_id is None:
    #     return APIResponse.error_with_code_message(message="Unauthorized")
    
    # Create a session
    session = Session()
    try:    
        json_data = request.get_json()
        parsed_opd_date = datetime.strptime(json_data["opd_date"], "%a, %d %b %Y %H:%M:%S %Z").date()

        month_start_date = parsed_opd_date.replace(day=1)
        next_month_start_date = (month_start_date + timedelta(days=32)).replace(day=1)
        month_end_date = next_month_start_date - timedelta(days=1)

        export_date_map = {month_start_date + timedelta(days=i): None for i in range((month_end_date - month_start_date).days + 1)}
        map_of_records = {}

        records = session.query(Record).filter(Record.opd_date >= month_start_date, Record.opd_date < month_end_date).all()

        export_data = [
            ["", "New Case", "Old Case", "Total Case"],
            ["Date", "0-15 Years", "16-60 years", "60 above", "total", "0-15 Years", "16-60 years", "60 above", "total", "0-15 Years", "16-60 years", "60 above", "total"],
            ["", "M","F", "M", "F", "M", "F", "M", "F", "M","F", "M", "F", "M", "F", "M", "F", "M","F", "M", "F", "M", "F", "M", "F"],
            # ["abc", 5, 10, 15, 20, 25, 30, 40,50, 5, 10, 15, 20, 25, 30, 40,50, 5, 10, 15, 20, 25, 30, 40,50],
            # ["Doe Joe", 35, "UK"]
        ]

        if non_null_non_empty_list(records):
            record_ids = []
            for record in records:
                record_ids.append(record.id)
                export_date_map[record.opd_date] = record
            
            groups = session.query(RecordGroup).filter(RecordGroup.record_id.in_(record_ids)).all()
            if non_null_non_empty_list(groups):
                for group in groups:
                    map_of_records.setdefault(group.record_id, []).append(group)
        
        for i in range((month_end_date - month_start_date).days + 1):
            date_row = month_start_date + timedelta(days=i)
            record = export_date_map.get(date_row)
            print(date_row)
            if record:
                print(record.id)

    finally:
        session.close()