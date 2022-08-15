import json
import psycopg2
from psycopg2 import Error
import logging
from airflow import DAG
import datetime as dt
from textwrap import dedent
import requests as req
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# [START default_args]

default_args = {
    'owner': 'airflowproject',
}
# [END default_args]

# [START instantiate_dag]
with DAG(
        'final_project',
        default_args=default_args,
        description='ETL DAG',
        schedule_interval=dt.timedelta(minutes=5),
        start_date=dt.datetime(2022, 8, 5),
        catchup=False,
        tags=['test'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]


    # [START extract_function]

    def data_load():
        res = req.get('https://apidata.mos.ru/v1/datasets/60865/rows?api_key=65a059a358b2497d6c87224f9d783c85')
        data = res.text

        data = data.replace('}},','},#').replace('}}','}').replace('[','').replace(']','').replace('"Cells":{','').split(',#')
        list_data =[]
        for i in range(len(data)):
            temp = json.loads(data[i])
            key = 'uniq_key'
            ts = dt.datetime.now()
            value = str(temp.get("global_id")) + "|" + str(ts)
            temp[key] = value
            key2 = 'processed_time'
            temp[key2] = ts
            list_data.append(temp)

        connection = psycopg2.connect(("""
            host=rc1b-tsmgwzxf6kio2ajk.mdb.yandexcloud.net
            port=6432
            dbname=stg
            user=user1
            password=Galateya_224
            target_session_attrs=read-write
            """))
        cursor = connection.cursor()

        def insert_database(uniq_key, processed_time, global_id, Number, NominationYear, Name, Author, PubYear, AgeLimit, PublishingHouse, LitPrizeName, Nomination): 
            
            
            insert_query = """ INSERT INTO stg_books (uniq_key, processed_time, global_id, number, nomitation_year, name, author, pub_year, age_limit, publishing_house, lit_prize_name, nomination) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            cursor.execute(insert_query, (uniq_key, processed_time, global_id, Number, NominationYear, Name, Author, PubYear, AgeLimit, PublishingHouse, LitPrizeName, Nomination))
            connection.commit()
          

        for i in range(len(list_data)):
            myjson = list_data[i]
            insert_database(myjson['uniq_key'], myjson['processed_time'], myjson['global_id'], myjson['Number'], myjson['NominationYear'], myjson['Name'], myjson['Author'], myjson['PubYear'], myjson['AgeLimit'], myjson['PublishingHouse'], myjson['LitPrizeName'], myjson['Nomination'])

        cursor.close()
        connection.close()

   # [START main_flow]
    extract_task = PythonOperator(
        task_id="data_load",
        python_callable=data_load)
 
#  # [START main_flow]
#     load_task = PythonOperator(
#         task_id="insert_database",
#         python_callable=insert_database)

data_load
