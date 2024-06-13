from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define los argumentos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define la función que será ejecutada por el operador
def print_current_datetime():
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Current time: {current_time}")

# Define el DAG
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='A simple DAG to print the current datetime',
    schedule_interval='0 0 * * *'  # Ejecutar diariamente a la medianoche UTC
)

# Define el operador que ejecuta la función
task_print_datetime = PythonOperator(
    task_id='print_current_datetime',
    python_callable=print_current_datetime,
    dag=dag
)
