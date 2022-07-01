from sqlalchemy import create_engine
import pandas as pd
import json
from peep import peepify
import psycopg2
from influxdb import InfluxDBClient, DataFrameClient
import json
from flask import Flask, request, jsonify
from sqlalchemy import create_engine
engine = create_engine("mysql+mysqlconnector://root:password@localhost/dbname", echo=True)
con=engine.connect()
app=Flask(__name__)


@app.route('/dlpreport', methods=['POST'])
def rgs():
    client = DataFrameClient(host='103.138.38.174', port=8086, username='root', password='567856',ssl=False, verify_ssl=False)
    conn = psycopg2.connect(dbname='indefendenterprise.5012969598',user = "postgres", password = "173rew", host = "143.639.78.174" , port=5432, sslmode='require')
    content=request.json
    lst=[]
    lst.append(content['from_date'])
    lst.append(content['to_date'])
    lst.append(content['filter_column'])
    lst.append(content['filter_value'])
    filepath=peepify(client, conn, lst)
    return jsonify({'Message': filepath})