from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'student',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def print_report_summary():
    """Print summary of the completed batch job"""
    print("="*60)
    print("Daily Traffic Analysis Report Generated")
    print(f"Report Date: {datetime.now().strftime('%Y-%m-%d')}")
    print("Peak traffic hours calculated for all junctions")
    print("Report available in PostgreSQL: peak_traffic_stats table")
    print("="*60)

with DAG(
    'smart_city_daily_report',
    default_args=default_args,
    description='Nightly aggregation of traffic data with peak hour analysis',
    schedule_interval='0 2 * * *',  # Runs at 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['traffic', 'batch', 'smart-city'],
) as dag:

    # Task 1: Run the Batch Analysis Script
    # Note: In Docker, paths are mounted to /opt/airflow
    calculate_peak_traffic = BashOperator(
        task_id='calculate_peak_traffic',
        bash_command='cd /opt/airflow && python src/batch_analizer.py',
        env={
            'PYSPARK_PYTHON': '/usr/local/bin/python3',
            'PYSPARK_DRIVER_PYTHON': '/usr/local/bin/python3'
        }
    )

    # Task 2: Generate Visualization Report
    generate_visualization = BashOperator(
        task_id='generate_visualization',
        bash_command='cd /opt/airflow && python src/visualize_traffic.py',
    )

    # Task 3: Print Summary
    print_summary = PythonOperator(
        task_id='print_summary',
        python_callable=print_report_summary
    )

    # Define task dependencies
    calculate_peak_traffic >> generate_visualization >> print_summary