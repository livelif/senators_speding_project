
import logging
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor


def handle_response(response):
    print("RESPONSE", response)
    if response.status_code == 200:
        logging.info("REQUEST SUCCESS")
        return True
    
    logging.error("REQUEST ERROR")
    return False

with DAG(
    'tutorial_request',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='A simple tutorial DAG',
    schedule_interval="@daily",
    start_date=datetime(2022, 9, 20),
    catchup=False,
    tags=['example'],
) as dag:
    # trocar para http sensor
    """bronze_senators = SimpleHttpOperator(task_id = "bronze_senators",
        method="GET",
        http_conn_id="request_lakehouse",
        endpoint="/extract/senators",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: handle_response(response),
        response_filter=lambda response: 'get', 
        dag=dag)
    
    bronze_votation_by_senator = SimpleHttpOperator(task_id = "bronze_votation_by_senator",
        method="GET",
        http_conn_id="request_lakehouse",
        endpoint="/extract/senators",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: handle_response(response),
        dag=dag)"""

    bronze_senators = HttpSensor(
        task_id = "bronze_senators",
        method="GET",
        http_conn_id="request_lakehouse",
        endpoint="/extract/senators",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: handle_response(response),
        dag=dag
    )
    
    bronze_votation_by_senator = HttpSensor(
        task_id = "bronze_votation_by_senator",
        method="GET",
        http_conn_id="request_lakehouse",
        endpoint="/extract/senators",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: handle_response(response),  
        dag=dag
    )
    
    silver_list = ["remuneration", "benefits", "senators", "votation_by_senator"]
    silver_list_operator = []
    for silver in silver_list:
        operator = SimpleHttpOperator(task_id = f"silver_{silver}",
            method="GET",
            http_conn_id="request_lakehouse",
            endpoint=f"/silver/{silver}",
            headers={"Content-Type": "application/json"},
            response_check=lambda response: handle_response(response),
            dag=dag)
        silver_list_operator.append(operator)

    silver_wait = DummyOperator(task_id='silver_wait', dag=dag)

    gold_list = ["senators_spent_with_benefits", "company_have_more_revenue", "employee_per_senator"]
    gold_list_operator = []
    for gold in gold_list:
        operator = SimpleHttpOperator(task_id = f"silver_{gold}",
            method="GET",
            http_conn_id="request_lakehouse",
            endpoint=f"/gold/{gold}",
            headers={"Content-Type": "application/json"},
            response_check=lambda response: handle_response(response),
            dag=dag)
        gold_list_operator.append(operator)

    gold_wait = DummyOperator(task_id='gold_wait', dag=dag)

    bronze_senators >> bronze_votation_by_senator >> silver_wait
    silver_wait >> silver_list_operator >> gold_wait
    gold_wait >> gold_list_operator