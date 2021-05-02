import os
from datetime import datetime, timedelta
import logging

#########################################################
#
#   Load Environment Variables
#
#########################################################

# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


########################################################
#
#   DAG Settings
#
#########################################################

from airflow import DAG

dag_default_args = {
    'owner': 'BDE_LAB_6',
    'start_date': datetime.now() - timedelta(days=1),
    'email': ['rogeryu@hotmail.com.au'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='exercise_2',
    default_args=dag_default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################


# Solution:
def _print_bde():
    logging.info("Big Data Engineering")
	
def _addition():
    logging.info(f"42 + 88 = {42+88}")
	
def _subtraction():
    logging.info(f"6 -2 = {6-2}")
	
def _division():
    logging.info(f"10 / 2 = {int(10/2)}")


#########################################################
#
#   DAG Operator Setup
#
#########################################################

from airflow.operators.python_operator import PythonOperator

print_bde = PythonOperator(
    task_id='print_bde_task',
    python_callable=_print_bde,
    dag=dag
)

addition = PythonOperator(
    task_id='addition_task',
    python_callable=_addition,
    dag=dag
)

subtraction = PythonOperator(
    task_id='subtraction',
    python_callable=_subtraction,
    dag=dag
)

division = PythonOperator(
    task_id='division',
    python_callable=_division,
    dag=dag
)

print_bde >> addition
print_bde >> subtraction
addition >> division
subtraction >> division