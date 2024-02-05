from flask import Flask, request, jsonify
import flask
from types import SimpleNamespace
import json
from sqlalchemy import Column, DateTime, Integer, String, create_engine, Date
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from datetime import datetime
from api_response import APIResponse

db_params = {
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'database': os.environ.get('DB_NAME'),
}

print(db_params)
# Construct the database URL
db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"

# Create the database engine
engine = create_engine(db_url, echo=True)

# Create a session factory
Session = sessionmaker(bind=engine)

# Create a session
session = Session()
Base = declarative_base()

def non_null_non_empty(data, key):
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

class RecordGroup(Base):
    __tablename__ = 'record_groups'
    id = Column(Integer, primary_key=True)
    name = Column(String, name="name")
    new_male = Column(Integer, name="new_male")
    new_female = Column(Integer, name="new_female")
    old_male = Column(Integer, name="old_male")
    old_female = Column(Integer, name="old_female")
    record_id = Column(Integer, name="record_id")


def insert_medical_record(request: flask.Request) -> flask.typing.ResponseReturnValue:
    # Use request.get_json() to get parsed JSON data
    json_data = request.get_json()

    # Convert the parsed JSON data to SimpleNamespace
    #data = json.loads(json.dumps(json_data), object_hook=lambda d: SimpleNamespace(**d))
    
    parsed_opd_date = datetime.strptime(json_data["opd_date"], "%a, %d %b %Y %H:%M:%S %Z").date()
    parsed_updated_at = datetime.strptime(json_data["updated_at"], "%a, %d %b %Y %H:%M:%S %Z")
    if not non_null_non_empty(json_data, "groups"): 
        return APIResponse.error_with_code_message(message="groups are not present cannot save")
    
    record_saved = Record(
        id=json_data.get("id"),
        opd_type=json_data["opd_type"],
        opd_date=parsed_opd_date,
        updated_at=parsed_updated_at
    )

    record_saved = session.merge(record_saved)
    session.query(RecordGroup).filter(RecordGroup.record_id == record_saved.id).delete()
    new_group_data = []
    for index, group in enumerate(json_data.get("groups")):
        name = ""
        if index == 0:
            name = "0-15 years"
        elif index == 1:
            name = "15-60 years"
        else:
            name = "60+ years"
        g = RecordGroup(name=name, new_male=group.get("new_male", 0), new_female=group.get("new_female", 0), old_male=group.get("old_male", 0), old_female=group.get("old_female", 0), record_id=record_saved.id)
        new_group_data.append(g)
    session.add_all(new_group_data)
    session.commit()

    return APIResponse.ok_with_data("data saved successfully")

