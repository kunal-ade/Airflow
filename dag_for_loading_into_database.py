from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import psycopg2 
from psql_details import *
import pendulum
from airflow.models import XCom



def loading_data(**kwargs):
    ti=kwargs['ti']
    xcom_value=ti.xcom_pull(task_ids='Reading_last_line_from_file')
    string_transformaiton =xcom_value.split(',')
    with psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_pwd,host=ip,port=5432) as conn:
        with conn.cursor() as cursor:
            with open('/home/kunal/airflow/dags/log.txt') as f:
                sql_command ="""
                            INSERT INTO airflow.my_automation (greeting,name,date,time_in_ist) VALUES (%s,%s,%s,%s)
                            """
                cursor.execute(sql_command,string_transformaiton)
    conn.commit()



file_path = '/home/kunal/airflow/dags/log.txt'


default_arg = {
    'owner': 'airflow',
    'retries': 0,
}

# Instantiate the DAG
with DAG(
    dag_id='Reading_and_loading_into_database',
    default_args=default_arg,
    description='Reading from a file and loading it into ',
    start_date=pendulum.datetime(2024,2,6,tz='Asia/Kolkata'),
    schedule_interval=None,
    catchup=False, 
    ) as dag:
    
    reading_last_line=BashOperator(
        task_id='Reading_last_line_from_file',
        bash_command=f'echo $(tail -n2 {file_path}) ',
        do_xcom_push=True,     
    )
    
    loading_into_db = PythonOperator(
        task_id='load_into_postgres',
        python_callable=loading_data,
        provide_context=True,      
    )
    reading_last_line >> loading_into_db





