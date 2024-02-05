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
    record_saved = Record(
        id=json_data.get("id"),
        opd_type=json_data["opd_type"],
        opd_date=parsed_opd_date,
        updated_at=parsed_updated_at
    )

    record_saved = session.merge(record_saved)
    session.query(RecordGroup).filter(RecordGroup.record_id == record_saved.id).delete()
    new_group_data = [
        RecordGroup(name='0-15 years', new_male=10, new_female=20, old_male=5, old_female=15, record_id=record_saved.id),
        RecordGroup(name='15-60 years', new_male=15, new_female=25, old_male=8, old_female=18, record_id=record_saved.id),
        RecordGroup(name='60+ years', new_male=15, new_female=25, old_male=8, old_female=18, record_id=record_saved.id),
    ]
    session.add_all(new_group_data)
    session.commit()

    return APIResponse.ok_with_data("data saved successfully")

