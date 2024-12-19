from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.state import State
import random
import time
from airflow.utils.dates import days_ago

# Function to force the DAG run's status to SUCCESS
def force_success_status(ti, **kwargs):
    dag_run = kwargs["dag_run"]
    dag_run.set_state(State.SUCCESS)

# Function to randomly select a medal type
def random_medal_choice():
    return random.choice(["Gold", "Silver", "Bronze"])

# Function to introduce an artificial delay
def delay_execution():
    time.sleep(20)

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

# MySQL connection ID
mysql_connection_id = "mysql_connection_zhenya"

# DAG definition
with DAG(
    "zhenya_datsenko_dag",
    default_args=default_args,
    schedule_interval=None,  # No regular schedule for this DAG
    catchup=False,  # Do not run for past dates
    tags=["zhenya_medal_counting"],  # DAG tags for categorization
) as dag:

    # Task 1: Create a table to store medal count results
    create_table_task = MySqlOperator(
        task_id="create_medal_table",
        mysql_conn_id=mysql_connection_id,
        sql="""
        CREATE TABLE IF NOT EXISTS neo_data.zhenya_datsenko_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            medal_count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Task 2: Randomly select a medal type
    select_medal_task = PythonOperator(
        task_id="select_medal",
        python_callable=random_medal_choice,
    )

    # Task 3: Branching logic based on the selected medal type
    def branching_logic(**kwargs):
        selected_medal = kwargs["ti"].xcom_pull(task_ids="select_medal")
        if selected_medal == "Gold":
            return "count_gold_medals"
        elif selected_medal == "Silver":
            return "count_silver_medals"
        else:
            return "count_bronze_medals"

    branching_task = BranchPythonOperator(
        task_id="branch_based_on_medal",
        python_callable=branching_logic,
        provide_context=True,
    )

    # Task 4: Count Bronze medals and insert results into the database
    count_bronze_task = MySqlOperator(
        task_id="count_bronze_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.zhenya_datsenko_medal_counts (medal_type, medal_count)
           SELECT 'Bronze', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Bronze';
           """,
    )

    # Task 5: Count Silver medals and insert results into the database
    count_silver_task = MySqlOperator(
        task_id="count_silver_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.zhenya_datsenko_medal_counts (medal_type, medal_count)
           SELECT 'Silver', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Silver';
           """,
    )

    # Task 6: Count Gold medals and insert results into the database
    count_gold_task = MySqlOperator(
        task_id="count_gold_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.zhenya_datsenko_medal_counts (medal_type, medal_count)
           SELECT 'Gold', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Gold';
           """,
    )

    # Task 7: Delay execution to simulate processing
    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS,  # Executes if at least one previous task succeeds
    )

    # Task 8: Check the most recent records using an SQL sensor
    check_last_record_task = SqlSensor(
        task_id="verify_recent_record",
        conn_id=mysql_connection_id,
        sql="""
            WITH count_in_medals AS (
                SELECT COUNT(*) as nrows 
                FROM neo_data.zhenya_datsenko_medal_counts
                WHERE created_at >= NOW() - INTERVAL 30 SECOND
            )
            SELECT nrows > 0 FROM count_in_medals; 
        """,
        mode="poke",  # Periodic condition check
        poke_interval=10,  # Check every 10 seconds
        timeout=30,  # Timeout after 30 seconds
    )

    # Task dependencies
    create_table_task >> select_medal_task >> branching_task
    (
        branching_task
        >> [count_bronze_task, count_silver_task, count_gold_task]
        >> delay_task
    )
    delay_task >> check_last_record_task