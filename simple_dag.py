from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt

default_args = {
    "owner" : "majeed",
    "start_date" : dt.datetime(2023,8,23),
    "retries" : 1,
    "retry_delay" : dt.timedelta(minutes=5),
}

dag = DAG("simple_dag",
          description='simple practice dag',
          default_args = default_args,
          schedule_interval=dt.timedelta(seconds=5),

)

task1 = BashOperator(
    task_id = 'print_hello',
    bash_command = 'echo "hello"',
    dag = dag,
)
task2 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

task1 >> task2