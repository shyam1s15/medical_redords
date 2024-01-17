from flask import Flask, request, jsonify
import flask
import logging
import psycopg2
from psycopg2 import sql
import os
from psycopg2.extras import RealDictCursor

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
        result = cursor.fetchone()
        return result

def detail_record(request: flask.Request)-> flask.typing.ResponseReturnValue:
    data = request.get_json()
    id = request.json.get('id', None)
    if id is None:
        return "error, id cannot be none", 500
    
    try: 
        connection = psycopg2.connect(**db_params)
        select_query = sql.SQL("""
                SELECT r.*, json_agg(g.*) AS groups
                FROM mo_records r
                LEFT JOIN record_groups g ON r.id = g.record_id
                where r.id = %(id)s
                GROUP BY r.id""")
        query_params = {
            "id": id
        }
        result = execute_query(connection, select_query, query_params)
        return jsonify({'results': result})
    except Exception as e:
            print(f"Error: {str(e)}")

