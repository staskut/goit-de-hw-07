from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import random
import time

MYSQL_CONN_ID = "goit_mysql_db_kutnyk"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staskut.medals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    medal_type VARCHAR(10),
    count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

INSERT_MEDAL_COUNT_SQL = """
INSERT INTO staskut.medals (medal_type, count)
SELECT 
    '{medal_type}', 
    COUNT(*) 
FROM olympic_dataset.athlete_event_results 
WHERE medal = '{medal_type}';
"""

CHECK_LATEST_RECORD_SQL = """
SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30 
FROM staskut.medals;
"""

def pick_medal(ti):
    selected_medal = random.choice(['Bronze', 'Silver', 'Gold'])
    ti.xcom_push(key='selected_medal', value=selected_medal)

def pick_medal_task(ti):
    selected_medal = ti.xcom_pull(task_ids='pick_medal', key='selected_medal')
    return f'calc_{selected_medal}'

def generate_delay():
    time.sleep(15)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
        'staskut_medal_count',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["staskut"]
) as dag:

    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=CREATE_TABLE_SQL
    )

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal_task
    )

    calc_bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=INSERT_MEDAL_COUNT_SQL.format(medal_type="Bronze")
    )

    calc_silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=INSERT_MEDAL_COUNT_SQL.format(medal_type="Silver")
    )

    calc_gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=MYSQL_CONN_ID,
        sql=INSERT_MEDAL_COUNT_SQL.format(medal_type="Gold")
    )

    generate_delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=MYSQL_CONN_ID,
        sql=CHECK_LATEST_RECORD_SQL,
        mode='poke',
        poke_interval=5,
        timeout=30,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_bronze, calc_silver, calc_gold]
    [calc_bronze, calc_silver, calc_gold] >> generate_delay_task >> check_for_correctness