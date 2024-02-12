from flask import Flask, request, jsonify
import flask
import logging
import psycopg2
from psycopg2 import sql
import os
from psycopg2.extras import RealDictCursor
from api_response import APIResponse

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
        return APIResponse.ok_with_data(result)
        # return jsonify({'results': result})
    except Exception as e:
            print(f"Error: {str(e)}")
    return APIResponse.error_with_message("Something went wrong")

