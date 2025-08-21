from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys
import os

# Add the scripts directory to Python path
sys.path.insert(0,'/opt/airflow/scripts')

# Import our custom Yahoo Finance stock fetcher
from stock_fetcher import YahooStockDataFetcher

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
    'yahoo_stock_data_pipeline',
    default_args=default_args,
    description='A pipeline to fetch and store stock market data from Yahoo Finance',
    schedule_interval='@daily', 
    max_active_runs=1,
    tags=['stock', 'yahoo-finance', 'etl']
)

def check_yahoo_api_connection(**context):
    """
    Check if we can connect to the Yahoo Finance API
    """
    import requests
    
    # Yahoo Finance test endpoint
    url = "https://query1.finance.yahoo.com/v8/finance/chart/AAPL"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    params = {
        'range': '1d',
        'interval': '1d'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if 'chart' not in data or not data['chart']['result']:
            raise Exception("Invalid response format from Yahoo Finance")
        
        print("✓ Yahoo Finance API connection successful")
        return True
        
    except Exception as e:
        print(f"✗ Yahoo Finance API connection failed: {e}")
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
            print("⚠ stock_data table does not exist, it will be created if needed")
        else:
            print("✓ stock_data table exists")
        
        # Check table structure (optional verification)
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'stock_data'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        if columns:
            print("Table structure:")
            for col_name, col_type in columns:
                print(f"  - {col_name}: {col_type}")
        
        cursor.close()
        conn.close()
        
        print("✓ Database connection verified")
        return True
        
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        raise

def fetch_and_store_stock_data(**context):
    """
    Main task to fetch and store stock data from Yahoo Finance
    """
    try:
        # Define symbols to fetch (you can customize this list)
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'NVDA', 'META', 'NFLX']
        
        # Get the data interval - default to 1 month for regular updates
        # For initial runs, you might want to use '3mo' or '6mo'
        period = context.get('dag_run').conf.get('period', '1mo') if context.get('dag_run') and context.get('dag_run').conf else '1mo'
        
        print(f"Fetching data for symbols: {symbols}")
        print(f"Period: {period}")
        
        # Initialize the Yahoo Finance stock fetcher
        fetcher = YahooStockDataFetcher()
        
        # Run the pipeline
        success = fetcher.run_pipeline(symbols, period)
        
        if not success:
            raise Exception("Yahoo Finance stock data pipeline completed with errors")
        
        print("✓ Yahoo Finance stock data pipeline completed successfully")
        return True
        
    except Exception as e:
        print(f"✗ Yahoo Finance stock data pipeline failed: {e}")
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
                MIN(date_recorded) as earliest_date,
                MAX(updated_at) as last_updated,
                AVG(close_price) as avg_close_price
            FROM stock_data 
            GROUP BY symbol 
            ORDER BY symbol;
        """)
        
        results = cursor.fetchall()
        
        print("\n" + "="*70)
        print("YAHOO FINANCE STOCK DATA PIPELINE SUMMARY REPORT")
        print("="*70)
        
        if results:
            print(f"{'Symbol':<8} {'Records':<8} {'Earliest':<12} {'Latest':<12} {'Avg Price':<10} {'Updated'}")
            print("-"*70)
            
            for symbol, count, latest_date, earliest_date, last_updated, avg_price in results:
                avg_price_str = f"${avg_price:.2f}" if avg_price else "N/A"
                print(f"{symbol:<8} {count:<8} {earliest_date} {latest_date} {avg_price_str:<10} {last_updated}")
        else:
            print("No data found in stock_data table")
        
        # Get total record count
        cursor.execute("SELECT COUNT(*) FROM stock_data;")
        total_count = cursor.fetchone()[0]
        
        # Get records updated in last 24 hours
        cursor.execute("""
            SELECT COUNT(*) FROM stock_data 
            WHERE updated_at >= NOW() - INTERVAL '24 hours';
        """)
        recent_updates = cursor.fetchone()[0]
        
        print(f"\nTotal records in database: {total_count}")
        print(f"Records updated in last 24 hours: {recent_updates}")
        print("="*70)
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to generate summary report: {e}")
        raise

def validate_data_freshness(**context):
    """
    Validate that we have recent data for our key symbols
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Check for recent data (within last 3 business days)
        cursor.execute("""
            SELECT 
                symbol,
                MAX(date_recorded) as latest_date,
                CURRENT_DATE - MAX(date_recorded) as days_old
            FROM stock_data 
            WHERE symbol IN ('AAPL', 'GOOGL', 'MSFT', 'TSLA')
            GROUP BY symbol
            HAVING CURRENT_DATE - MAX(date_recorded) > 5;
        """)
        
        stale_data = cursor.fetchall()
        
        if stale_data:
            print("⚠ Warning: Stale data detected:")
            for symbol, latest_date, days_old in stale_data:
                print(f"  {symbol}: {days_old} days old (latest: {latest_date})")
        else:
            print("✓ All key symbols have recent data")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Data freshness validation failed: {e}")
        raise

# Task 1: Check Yahoo Finance API Connection
check_api_task = PythonOperator(
    task_id='check_yahoo_api_connection',
    python_callable=check_yahoo_api_connection,
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
        -- Data Quality Checks for Yahoo Finance Data
        WITH quality_metrics AS (
            SELECT 
                symbol,
                COUNT(*) as total_records,
                COUNT(CASE WHEN open_price IS NULL THEN 1 END) as null_opens,
                COUNT(CASE WHEN close_price IS NULL THEN 1 END) as null_closes,
                COUNT(CASE WHEN high_price IS NULL THEN 1 END) as null_highs,
                COUNT(CASE WHEN low_price IS NULL THEN 1 END) as null_lows,
                COUNT(CASE WHEN volume IS NULL THEN 1 END) as null_volumes,
                MIN(date_recorded) as earliest_date,
                MAX(date_recorded) as latest_date,
                -- Check for impossible price relationships
                COUNT(CASE WHEN high_price < low_price THEN 1 END) as invalid_high_low,
                COUNT(CASE WHEN open_price < 0 OR close_price < 0 THEN 1 END) as negative_prices
            FROM stock_data 
            WHERE date_recorded >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY symbol
        )
        SELECT 
            symbol,
            total_records,
            null_opens,
            null_closes,
            null_highs,
            null_lows,
            null_volumes,
            earliest_date,
            latest_date,
            invalid_high_low,
            negative_prices,
            CASE 
                WHEN null_opens + null_closes + null_highs + null_lows > 0 THEN 'WARNING: NULL values detected'
                WHEN invalid_high_low > 0 THEN 'ERROR: Invalid price relationships'
                WHEN negative_prices > 0 THEN 'ERROR: Negative prices detected'
                ELSE 'PASS'
            END as quality_status
        FROM quality_metrics
        ORDER BY symbol;
    """,
    dag=dag
)

# Task 6: Data Freshness Validation
freshness_task = PythonOperator(
    task_id='validate_data_freshness',
    python_callable=validate_data_freshness,
    dag=dag
)

# Define task dependencies
# Run API and DB checks in parallel, then fetch data, then run all validation tasks in parallel
[check_api_task, check_db_task] >> fetch_data_task >> [summary_task, data_quality_task, freshness_task]