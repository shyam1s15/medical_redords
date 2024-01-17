from flask import Flask, request, jsonify
import flask
import psycopg2
from psycopg2 import sql
import os
from psycopg2.extras import RealDictCursor
from types import SimpleNamespace
import json

db_params = {
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'database': os.environ.get('DB_NAME'),
}

def execute_query(connection, query, params=None):
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
        return result

def execute_insert_query(data):
    # Connect to the PostgreSQL database
    if data is None or not data:
        return 1 
    resp = 1
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cursor:
            try:
                # Start a transaction
                conn.autocommit = False

                # Insert data into the "main_table"
                sql_main = """
                    INSERT INTO mo_records (opd_date, opd_type, updated)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    RETURNING id;
                """

                cursor.execute(sql_main, (data.opd_date, data.opd_type))
                record_id = cursor.fetchone()[0]
                

                print(record_id)
                # Insert data into the "groups_table" using the generated id
                sql_groups = "INSERT INTO record_groups (name, male, female, record_id) VALUES (%s, %s, %s, %s)"
                for group in data.groups:
                    cursor.execute(sql_groups, (group.name, group.male, group.female, record_id))

                # Commit the transaction
                conn.commit()
                print("Data inserted successfully.")

            except Exception as e:
                # Rollback the transaction if an error occurs
                conn.rollback()
                print(f"Error: {e}")
                resp = 1

            finally:
                # Restore autocommit to its default state
                conn.autocommit = True
    resp = 0
    return resp


def execute_update_query(data):
    if data is None or not data:
        return 1 
    resp = 1
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cursor:
            try:
                # Start a transaction
                conn.autocommit = False

                # Insert data into the "main_table"
                sql_main = """
                    UPDATE mo_records set opd_date = %(opd_date)s, opd_type=%(opd_type)s, updated=CURRENT_TIMESTAMP
                    where id = %(id)s
                """
                sql_body = {
                    'opd_date': data.opd_date,
                    'opd_type': data.opd_type,
                    'id': data.id
                }
                cursor.execute(sql_main, sql_body)

                # Insert data into the "groups_table" using the generated id
                sql_groups = """
                     UPDATE record_groups
                        SET male = %(male)s, female = %(female)s
                        WHERE id = %(id)s
                """
                for group in data.groups:
                    group_data = {
                        'name': group.name,
                        'male': group.male,
                        'female': group.female,
                        'id': group.id,
                    }
                    cursor.execute(sql_groups, group_data)
                # Commit the transaction
                conn.commit()
                print("Data updated successfully.")

            except Exception as e:
                # Rollback the transaction if an error occurs
                conn.rollback()
                print(f"Error: {e}")
                resp = 1

            finally:
                # Restore autocommit to its default state
                conn.autocommit = True
    resp = 0
    return resp

    

def insert_medical_record(request: flask.Request) -> flask.typing.ResponseReturnValue:
    # Use request.get_json() to get parsed JSON data
    json_data = request.get_json()

    # Convert the parsed JSON data to SimpleNamespace
    data = json.loads(json.dumps(json_data), object_hook=lambda d: SimpleNamespace(**d))
    
    # Access attributes and print
    print(data)
    if hasattr(data, 'groups') and len(data.groups) > 0:
        record_id = getattr(data, 'id', None)
        if record_id == None:
            print("id is none")
            resp = execute_insert_query(data=data)
        else:
            print("id is not none")
            resp = execute_update_query(data=data)
    else:    
        print("No groups present.")
        return "something bad", 500
    # Return a response (optional)
         # Convert SimpleNamespace to dictionary and return as JSON response
    
    return "something : " + str(resp), 200
