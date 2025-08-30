"""
Finance Data Pipeline DAG
Orchestrates the complete data pipeline from ingestion to analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
import logging

# Default arguments
default_args = {
    'owner': 'finance-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'finance_data_pipeline',
    default_args=default_args,
    description='Complete finance data pipeline with Spark and DBT',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    catchup=False,
    tags=['finance', 'data-pipeline', 'spark', 'dbt'],
)

# Task 1: Data Ingestion
def ingest_market_data(**context):
    """Ingest market data from various sources."""
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from src.ingestion.yahoo_finance import YahooFinanceIngestion
    import asyncio
    
    async def fetch_data():
        ingestion = YahooFinanceIngestion()
        symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
        
        for symbol in symbols:
            result = await ingestion.fetch_data(symbol)
            if result.success:
                logging.info(f"Successfully ingested data for {symbol}")
            else:
                logging.error(f"Failed to ingest data for {symbol}: {result.error}")
        
        await ingestion.close()
    
    asyncio.run(fetch_data())
    return "Data ingestion completed"

ingest_task = PythonOperator(
    task_id='ingest_market_data',
    python_callable=ingest_market_data,
    dag=dag,
)

# Task 2: Spark Data Processing
spark_process_task = SparkSubmitOperator(
    task_id='spark_process_market_data',
    application='/opt/airflow/src/spark_jobs/process_market_data.py',
    conn_id='spark_default',
    conf={
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
        'spark.executor.cores': '1',
        'spark.sql.adaptive.enabled': 'true',
    },
    dag=dag,
)

# Task 3: DBT Transformations
dbt_run_task = BashOperator(
    task_id='dbt_run_transformations',
    bash_command='cd /opt/airflow && dbt run --profiles-dir .dbt',
    dag=dag,
)

# Task 4: Data Quality Tests
dbt_test_task = BashOperator(
    task_id='dbt_test_data_quality',
    bash_command='cd /opt/airflow && dbt test --profiles-dir .dbt',
    dag=dag,
)

# Task 5: Great Expectations Validation
def run_great_expectations(**context):
    """Run Great Expectations data quality checks."""
    import subprocess
    import sys
    
    try:
        result = subprocess.run([
            'great_expectations', 'checkpoint', 'run', 'finance_data_checkpoint'
        ], capture_output=True, text=True, cwd='/opt/airflow/great_expectations')
        
        if result.returncode == 0:
            logging.info("Great Expectations validation passed")
            return "Data quality validation passed"
        else:
            logging.error(f"Great Expectations validation failed: {result.stderr}")
            raise Exception("Data quality validation failed")
            
    except Exception as e:
        logging.error(f"Error running Great Expectations: {e}")
        raise

ge_validation_task = PythonOperator(
    task_id='great_expectations_validation',
    python_callable=run_great_expectations,
    dag=dag,
)

# Task 6: Generate Analytics Reports
def generate_analytics_reports(**context):
    """Generate analytics reports and metrics."""
    import pandas as pd
    import psycopg2
    from datetime import datetime
    
    # Connect to database
    conn = psycopg2.connect(
        host="postgres",
        database="finance_data",
        user="postgres",
        password="postgres"
    )
    
    # Generate daily summary
    query = """
    SELECT 
        symbol,
        date_trunc('day', timestamp) as date,
        count(*) as tick_count,
        avg(price) as avg_price,
        min(price) as low_price,
        max(price) as high_price,
        sum(volume) as total_volume
    FROM tick_data 
    WHERE timestamp >= current_date - interval '1 day'
    GROUP BY symbol, date_trunc('day', timestamp)
    ORDER BY symbol, date
    """
    
    df = pd.read_sql(query, conn)
    
    # Save report
    report_path = f"/opt/airflow/reports/daily_summary_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(report_path, index=False)
    
    conn.close()
    
    logging.info(f"Analytics report generated: {report_path}")
    return f"Report generated: {report_path}"

analytics_task = PythonOperator(
    task_id='generate_analytics_reports',
    python_callable=generate_analytics_reports,
    dag=dag,
)

# Task 7: Cleanup Old Data
cleanup_task = PostgresOperator(
    task_id='cleanup_old_data',
    postgres_conn_id='postgres_default',
    sql="""
    DELETE FROM tick_data 
    WHERE timestamp < current_date - interval '30 days';
    """,
    dag=dag,
)

# Task 8: Success Notification
def send_success_notification(**context):
    """Send success notification."""
    logging.info("Pipeline completed successfully!")
    return "Pipeline completed successfully"

success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
)

# Define task dependencies
ingest_task >> spark_process_task >> dbt_run_task >> dbt_test_task >> ge_validation_task
ge_validation_task >> analytics_task >> cleanup_task >> success_notification
