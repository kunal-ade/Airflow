from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import random 
defualt_arg={
    'owner':'kunal',
    'reties':0,
    }
greeting_list=["Good morning","Good afternoon","Good evening","Hie","Hello"]
name_list=["John","Tyrion","Jesse","Rose","Arya"]

def write_to_file(**kwargs):
    name=kwargs['name']
    greeting=kwargs['greeting']
    with open('/home/kunal/airflow/dags/log.txt','a') as file:
        current_date=pendulum.now(tz='Asia/Kolkata').strftime("%Y-%m-%d")
        current_time=pendulum.now(tz='Asia/Kolkata').strftime("%H:%M:%S")
        file.write(f"{greeting}, {name}, {current_date} ,{current_time}\n ")

with DAG(
    dag_id='Open_and_write_line_into_file',
    description='Appending a message every minute  into a file',
    default_args=defualt_arg,
    start_date=pendulum.datetime(2024,2,6,tz='Asia/Kolkata'),
    schedule_interval='*/1 * * * *',
    catchup=False,
) as dag:
    write_task=PythonOperator(
        task_id='writing_to_file',
        python_callable=write_to_file,
        op_kwargs={'name':random.choice(name_list),'greeting':random.choice(greeting_list)},
        dag=dag,
    )
    trigger_dag2_task = TriggerDagRunOperator(
        task_id='trigger_dag2',
        trigger_dag_id='Reading_and_loading_into_database',
        wait_for_completion=True,
        execution_date=pendulum.datetime(2024,2,6,tz='Asia/Kolkata'),
        reset_dag_run=True,

    )
    

