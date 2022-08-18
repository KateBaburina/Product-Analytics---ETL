from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Функция для CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
                     }

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-baburina-9',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 5),
}

# Интервал запуска DAG
schedule_interval = '0 10 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl_baburina():

    @task()
    def extract_feed():
        query = """SELECT 
               toDate(time) as event_date,
               user_id,
               gender, age, os,
               countIf(user_id, action = 'view') AS views,
               countIf(user_id, action = 'like') AS likes
            FROM 
                simulator_20220720.feed_actions 
            where 
                toDate(time) = yesterday()
            group by
                event_date,
                user_id,
                gender, age, os
                format TSVWithNames"""
        df_cube = ch_get_df(query=query)
        return df_cube
    
    @task()
    def extract_messenger():
        query = """SELECT event_date, user_id, gender, age, os, 
                          users_sent, messages_sent, users_received, messages_received
                          FROM (
                          SELECT toDate(time) as event_date, user_id, gender, age, os,
                                count(distinct reciever_id) as users_sent, count(*) as messages_sent
                                FROM 
                            simulator_20220720.message_actions
                            WHERE 
                            toDate(time) = yesterday()
                            GROUP BY
                            event_date, user_id, gender, age, os) t1
                            JOIN 
                            (SELECT  toDate(time) as event_date, reciever_id as user_id, gender, os,
                                count(distinct(user_id)) as users_received, count(*) as messages_received
                                FROM simulator_20220720.message_actions
                                WHERE toDate(time) = yesterday()
                                GROUP BY event_date, user_id, gender, age, os) t2
                                ON t1.user_id = t2.user_id
                                format TSVWithNames"""
        df_cube = ch_get_df(query=query)
        return df_cube

    @task()
    def transform_combined(df_cube_feed, df_cube_messenger):
        df_combined = df_cube_feed.merge(df_cube_messenger, on = ['event_date', 'user_id', 'gender', 'age', 'os'], how = 'outer')
        return df_combined
    
    @task()
    def transform_gender(df_combined):
        df_combined_gender = df_combined.copy()
        df_combined_gender['dimension'] = 'gender'
        df_combined_gender = df_combined_gender[['event_date', 'dimension', 'gender', 'likes','views', 'users_sent', 'messages_sent', 'users_received', 'messages_received']].groupby(['event_date', 'dimension', 'gender']).sum().reset_index().rename(columns = {'gender':'dimension_values'})
        return df_combined_gender
    
    @task()
    def transform_age(df_combined):
        df_combined_age = df_combined.copy()
        df_combined_age['dimension'] = 'age'
        df_combined_age = df_combined_age[['event_date', 'dimension', 'age', 'likes', 'views', 'users_sent', 'messages_sent', 'users_received', 'messages_received']].groupby(['event_date', 'dimension', 'age']).sum().reset_index().rename(columns = {'age':'dimension_values'})
        return df_combined_age
    
    @task()
    def transform_os(df_combined):
        df_combined_os = df_combined.copy()
        df_combined_os['dimension'] = 'os'
        df_combined_os = df_combined_os[['event_date', 'dimension', 'os', 'likes', 'views', 'users_sent', 'messages_sent', 'users_received', 'messages_received']].groupby(['event_date', 'dimension', 'os']).sum().reset_index().rename(columns = {'os':'dimension_values'})
        return df_combined_os
    
    @task
    def transform_final(df_combined_gender, df_combined_age, df_combined_os):
        df_final = pd.concat([df_combined_gender, df_combined_age, df_combined_os]).reset_index(drop=True)
        return df_final
    
    @task
    def load(df_final):
        df_final['event_date'] = pd.to_datetime(df_final['event_date'])
        df_final = df_final.astype({'dimension':'object', 'dimension_values':'object', 'likes': int, 'views': int, 'users_sent': int,
                                    'messages_sent': int, 'users_received': int, 'messages_received': int})

        ph.to_clickhouse(df_final, 'ebaburina2', index=False, connection=connection_test)


    df_cube_feed = extract_feed()
    df_cube_messenger = extract_messenger()
    df_combined = transform_combined(df_cube_feed, df_cube_messenger)
    df_combined_gender = transform_gender(df_combined)
    df_combined_age = transform_age(df_combined)
    df_combined_os = transform_os(df_combined)
    
    df_final = transform_final(df_combined_gender, df_combined_age, df_combined_os)
    
    load(df_final)
    

dag_etl_baburina = dag_etl_baburina()

