from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
import os
import re

# Get the directory of the current DAG file
dag_dir = os.path.dirname(os.path.abspath(__file__))


def read_spark_result(**context):
    with (open('/tmp/airflow_spark_result.txt', 'r') as f):
        result = f.read().strip() #.replace('\n', '|')
    result = re.sub(r'\r*\n+\r*', ',', result)
    # Store in Airflow Variables for the next task to use
    from airflow.models import Variable
    Variable.set("spark_task_result", result)
    os.remove('/tmp/airflow_spark_result.txt')

    return result

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
}

with DAG(
    dag_id='valantsevych_goit_de_fp_dag',
    default_args=default_args,
    schedule_interval='*/30 * * * *',  # запуск щохвилини
    catchup=False,
    tags=['valantsevych_goit_de_fp_tag'],
) as dag:
    read_task = PythonOperator(
        task_id='read_result',
        python_callable=read_spark_result,
        provide_context=True,
        dag=dag)
    landing_to_bronze = SparkSubmitOperator(
        application=os.path.join(dag_dir, 'p2_landing_to_bronze.py'),
        task_id='landing_to_bronze',
        conn_id='spark-default',
        verbose=True,
        dag=dag)
    bronze_to_silver = SparkSubmitOperator(
        application=os.path.join(dag_dir, 'p2_bronze_to_silver.py'),
        application_args=['--prev', '{{ var.value.spark_task_result }}'],
        task_id='bronze_to_silver',
        conn_id='spark-default',
        verbose=True,
        dag=dag)
    silver_to_gold = SparkSubmitOperator(
        application=os.path.join(dag_dir, 'p2_silver_to_gold.py'),
        application_args=['--prev', '{{ var.value.spark_task_result }}'],
        task_id='silver_to_gold',
        conn_id='spark-default',
        verbose=True,
        dag=dag)

    # Встановлення залежностей
    landing_to_bronze >> read_task >> bronze_to_silver >> silver_to_gold

