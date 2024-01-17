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
        result = cursor.fetchall()
        return result
    
def fetch_records_list(request: flask.Request)-> flask.typing.ResponseReturnValue:
    data = request.get_json()
    page_id = request.json.get('page', {}).get('page_id', 0)
    page_limit = request.json.get('page', {}).get('page_limit', 20)
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(**db_params)
        print(page_id)
        print(page_limit)
        # Example SELECT query with named placeholders for pagination
        select_query = sql.SQL("""
                SELECT r.*, json_agg(g.*) AS groups
                FROM mo_records r
                LEFT JOIN record_groups g ON r.id = g.record_id
                GROUP BY r.id LIMIT %(limit)s OFFSET %(offset)s""")

        # Parameters for the query
        query_params = {'limit': page_limit, 'offset': page_id*page_limit}

        # Execute the query and fetch results
        result = execute_query(connection, select_query, query_params)

        return APIResponse.ok_with_data({'results': result})

    except Exception as e:
        print(f"Error: {str(e)}")
        return APIResponse.error_with_code_message("something went wrong ::: " + str(e))

    return APIResponse.error_with_code_message("something went wrong")
