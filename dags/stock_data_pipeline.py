from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys
import os

# Add the scripts directory to Python path
sys.path.insert(0, '/opt/airflow/scripts')

# Import our custom stock fetcher
from stock_fetcher import StockDataFetcher

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='A pipeline to fetch and store stock market data',
    schedule_interval=timedelta(hours=1),  # Run every hour
    max_active_runs=1,
    tags=['stock', 'api', 'etl']
)

def check_api_connection(**context):
    """
    Check if we can connect to the Alpha Vantage API
    """
    import requests
    import os
    
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError("API key not found in environment variables")
    
    # Test API connection with a lightweight request
    url = "https://www.alphavantage.co/query"
    params = {
        'function': 'GLOBAL_QUOTE',
        'symbol': 'AAPL',
        'apikey': api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'Error Message' in data:
            raise Exception(f"API Error: {data['Error Message']}")
        
        print("✓ API connection successful")
        return True
        
    except Exception as e:
        print(f"✗ API connection failed: {e}")
        raise

def check_database_connection(**context):
    """
    Check PostgreSQL database connection and table existence
    """
    try:
        # Use Airflow's PostgresHook for connection
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Test connection
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Check if our table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'stock_data'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            raise Exception("stock_data table does not exist")
        
        cursor.close()
        conn.close()
        
        print("✓ Database connection and table verified")
        return True
        
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        raise

def fetch_and_store_stock_data(**context):
    """
    Main task to fetch and store stock data
    """
    try:
        # Define symbols to fetch
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
        
        # Initialize the stock fetcher
        fetcher = StockDataFetcher()
        
        # Run the pipeline
        success = fetcher.run_pipeline(symbols)
        
        if not success:
            raise Exception("Stock data pipeline completed with errors")
        
        print("✓ Stock data pipeline completed successfully")
        return True
        
    except Exception as e:
        print(f"✗ Stock data pipeline failed: {e}")
        raise

def generate_summary_report(**context):
    """
    Generate a summary report of the data update
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Get count of records by symbol
        cursor.execute("""
            SELECT 
                symbol, 
                COUNT(*) as record_count,
                MAX(date_recorded) as latest_date,
                MAX(updated_at) as last_updated
            FROM stock_data 
            GROUP BY symbol 
            ORDER BY symbol;
        """)
        
        results = cursor.fetchall()
        
        print("\n" + "="*50)
        print("STOCK DATA PIPELINE SUMMARY REPORT")
        print("="*50)
        
        for symbol, count, latest_date, last_updated in results:
            print(f"{symbol}: {count} records | Latest: {latest_date} | Updated: {last_updated}")
        
        # Get total record count
        cursor.execute("SELECT COUNT(*) FROM stock_data;")
        total_count = cursor.fetchone()[0]
        
        print(f"\nTotal records in database: {total_count}")
        print("="*50)
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to generate summary report: {e}")
        raise

# Task 1: Check API Connection
check_api_task = PythonOperator(
    task_id='check_api_connection',
    python_callable=check_api_connection,
    dag=dag
)

# Task 2: Check Database Connection
check_db_task = PythonOperator(
    task_id='check_database_connection', 
    python_callable=check_database_connection,
    dag=dag
)

# Task 3: Fetch and Store Stock Data
fetch_data_task = PythonOperator(
    task_id='fetch_and_store_stock_data',
    python_callable=fetch_and_store_stock_data,
    dag=dag
)

# Task 4: Generate Summary Report
summary_task = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    dag=dag
)

# Task 5: Data Quality Check
data_quality_task = PostgresOperator(
    task_id='data_quality_check',
    postgres_conn_id='postgres_default',
    sql="""
        SELECT 
            symbol,
            COUNT(*) as record_count,
            COUNT(CASE WHEN open_price IS NULL THEN 1 END) as null_opens,
            COUNT(CASE WHEN close_price IS NULL THEN 1 END) as null_closes,
            MIN(date_recorded) as earliest_date,
            MAX(date_recorded) as latest_date
        FROM stock_data 
        WHERE date_recorded >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY symbol;
    """,
    dag=dag
)

# Define task dependencies
check_api_task >> check_db_task >> fetch_data_task >> [summary_task, data_quality_task]