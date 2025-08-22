from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys
import requests

sys.path.insert(0, "/opt/airflow/scripts")

# Import  custom Yahoo Finance stock fetcher
from stock_fetcher import YahooStockDataFetcher

# ========================================
# USER CONFIGURATION SECTION
# ========================================

# Stock symbols to fetch (customize as needed)
STOCK_SYMBOLS = [
    "RELIANCE.NS",
    "TCS.NS",
    "INFY.NS",
    "HDFCBANK.NS",
    "ICICIBANK.NS",
    "HINDUNILVR.NS",
    "SBIN.NS",
    "BAJFINANCE.NS",
]


# Data period to fetch from Yahoo Finance API
# Options: '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'
DEFAULT_PERIOD = "3mo"
# Data fetch interval (granularity of stock data)
DATA_INTERVAL = "1d"  # or ['1m', '5m','1h','1d','1wk','1mo']

# ========================================
# SCHEDULING CONFIGURATION
# ========================================

# DAG Start Date Options:
# Option 1: Start Date
DAG_START_DATE = datetime.now()

# Option 2: Start from specific date (uncomment to use)
# DAG_START_DATE = datetime(2024, 12, 1)

# Option 3: Start from a relative date (uncomment to use)
# DAG_START_DATE = datetime.now() - timedelta(days=30)  # 30 days ago

# ========================================
# SCHEDULE INTERVAL OPTIONS FOR DAG
# ========================================

# Choose your schedule (uncomment ONE option):

# Daily scheduling 
SCHEDULE_INTERVAL = "@daily"  # Runs everyay at 5:30pm IST an 00:00 UTC

# Hourly scheduling 
# SCHEDULE_INTERVAL = "@hourly"

# Custom cron expressions:
# SCHEDULE_INTERVAL = '0 9 * * 1-5'    # 9 AM, Monday to Friday
# SCHEDULE_INTERVAL = '0 */4 * * *'    # Every 4 hours
# SCHEDULE_INTERVAL = '30 15 * * 1-5'  # 3:30 PM, weekdays only

# Manual trigger only (no automatic scheduling)
# SCHEDULE_INTERVAL = None

# ========================================
# ADVANCED OPTIONS
# ========================================

# Catchup behavior
# True: Run missed DAG instances for historical dates
# False: Only run from start_date forward (recommended)
ENABLE_CATCHUP = False

# Maximum concurrent DAG runs
MAX_ACTIVE_RUNS = 1

# Email notifications (set to True if you want email alerts)
EMAIL_ON_FAILURE = False
EMAIL_ON_RETRY = False

# Retry configuration
RETRIES = 2
RETRY_DELAY_MINUTES = 5

# ========================================
# DAG CONFIGURATION (DO NOT MODIFY)
# ========================================

# Default arguments for the DAG
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": DAG_START_DATE,
    "email_on_failure": EMAIL_ON_FAILURE,
    "email_on_retry": EMAIL_ON_RETRY,
    "retries": RETRIES,
    "retry_delay": timedelta(minutes=RETRY_DELAY_MINUTES),
    "catchup": ENABLE_CATCHUP,
}

# Create the DAG
dag = DAG(
    "yahoo_stock_data_pipeline",
    default_args=default_args,
    description="Simplified pipeline to fetch and store stock data from Yahoo Finance",
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["stock", "yahoo-finance", "etl"],
)


def check_yahoo_api_connection(**context):
    """Check if Yahoo Finance API is accessible"""
    print("🔍 Testing Yahoo Finance API connection...")

    url = "https://query1.finance.yahoo.com/v8/finance/chart/AAPL"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    params = {"range": "1d", "interval": "1d"}

    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()

        if "chart" not in data or not data["chart"]["result"]:
            raise Exception("Invalid response format from Yahoo Finance")

        print("✅ Yahoo Finance API connection successful")
        return True

    except Exception as e:
        print(f"❌ Yahoo Finance API connection failed: {e}")
        raise


def check_database_connection(**context):
    """Check PostgreSQL database connection and verify table"""
    print("🔍 Testing database connection...")

    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Check if stock_data table exists
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'stock_data'
            );
        """
        )

        table_exists = cursor.fetchone()[0]

        if table_exists:
            print("✅ stock_data table exists")

            # Show table structure
            cursor.execute(
                """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'stock_data'
                ORDER BY ordinal_position;
            """
            )

            columns = cursor.fetchall()
            print("📋 Table structure:")
            for col_name, col_type in columns:
                print(f"   • {col_name}: {col_type}")
        else:
            print("⚠️  stock_data table does not exist - will be created if needed")

        cursor.close()
        conn.close()
        print("✅ Database connection verified")
        return True

    except Exception as e:
        print(f" Database connection failed: {e}")
        raise


def fetch_and_store_stock_data(**context):
    """Main task to fetch and store stock data from Yahoo Finance"""
    print(" Starting stock data fetch and store process...")

    try:
        # Get period from DAG run config or use default
        dag_run = context.get("dag_run")
        period = DEFAULT_PERIOD
        interval = DATA_INTERVAL

        if dag_run and dag_run.conf:
            period = dag_run.conf.get("period", DEFAULT_PERIOD)

        print(f" Fetching data for symbols: {STOCK_SYMBOLS}")
        print(f" Period: {period}")
        print(f" Interval: {interval}")
        # Initialize the Yahoo Finance stock fetcher
        fetcher = YahooStockDataFetcher()

        # Run the pipeline
        success = fetcher.run_pipeline(STOCK_SYMBOLS, period, interval)

        if not success:
            raise Exception("Stock data pipeline completed with errors")

        print("✅ Stock data pipeline completed successfully")
        return True

    except Exception as e:
        print(f"❌ Stock data pipeline failed: {e}")
        raise


def generate_summary_report(**context):
    """Generate a summary report of the data update"""
    print("📋 Generating summary report...")

    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Get summary statistics by symbol
        cursor.execute(
            """
            SELECT 
                symbol, 
                COUNT(*) as record_count,
                MIN(date_recorded) as earliest_date,
                MAX(date_recorded) as latest_date,
                ROUND(CAST(AVG(close_price) AS numeric), 2) as avg_close_price,
                MAX(updated_at) as last_updated
            FROM stock_data 
            WHERE date_recorded >= CURRENT_DATE - INTERVAL '60 days'  -- Last 2 months
            GROUP BY symbol 
            ORDER BY symbol;
        """
        )

        results = cursor.fetchall()

        print("\n" + "=" * 80)
        print("📈 YAHOO FINANCE STOCK DATA PIPELINE SUMMARY")
        print("=" * 80)

        if results:
            print(
                f"{'Symbol':<8} {'Records':<8} {'From':<12} {'To':<12} {'Avg Price':<12} {'Updated'}"
            )
            print("-" * 80)

            for symbol, count, earliest, latest, avg_price, updated in results:
                avg_price_str = f"${avg_price}" if avg_price else "N/A"
                print(
                    f"{symbol:<8} {count:<8} {earliest} {latest} {avg_price_str:<12} {updated.strftime('%Y-%m-%d %H:%M')}"
                )
        else:
            print("⚠️  No recent data found in stock_data table")

        # Get total statistics
        cursor.execute("SELECT COUNT(*) FROM stock_data;")
        total_count = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(*) FROM stock_data 
            WHERE updated_at >= NOW() - INTERVAL '24 hours';
        """
        )
        recent_updates = cursor.fetchone()[0]

        print(f"\n📊 Total records in database: {total_count:,}")
        print(f"🔄 Records updated in last 24 hours: {recent_updates}")
        print("=" * 80 + "\n")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Failed to generate summary report: {e}")
        raise


def validate_data_freshness(**context):
    """Validate that we have recent data for key symbols"""
    print("🔍 Validating data freshness...")

    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Check for stale data (older than 5 days for key symbols)
        cursor.execute(
            """
            SELECT 
                symbol,
                MAX(date_recorded) AS latest_date,
                EXTRACT(DAY FROM (CURRENT_DATE - MAX(date_recorded))) AS days_old
            FROM stock_data 
            WHERE symbol IN ('AAPL', 'GOOGL', 'MSFT', 'TSLA')
            GROUP BY symbol
            HAVING (CURRENT_DATE - MAX(date_recorded)) > INTERVAL '5 days';
        """
        )

        stale_data = cursor.fetchall()

        if stale_data:
            print("⚠️  Warning: Stale data detected for key symbols:")
            for symbol, latest_date, days_old in stale_data:
                print(f"   • {symbol}: {days_old} days old (latest: {latest_date})")
        else:
            print("✅ All key symbols have recent data (within 5 days)")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Data freshness validation failed: {e}")
        raise


# Define Tasks

# Task 1: Check Yahoo Finance API Connection
check_api_task = PythonOperator(
    task_id="check_yahoo_api_connection",
    python_callable=check_yahoo_api_connection,
    dag=dag,
)

# Task 2: Check Database Connection
check_db_task = PythonOperator(
    task_id="check_database_connection",
    python_callable=check_database_connection,
    dag=dag,
)

# Task 3: Fetch and Store Stock Data
fetch_data_task = PythonOperator(
    task_id="fetch_and_store_stock_data",
    python_callable=fetch_and_store_stock_data,
    dag=dag,
)

# Task 4: Generate Summary Report
summary_task = PythonOperator(
    task_id="generate_summary_report", python_callable=generate_summary_report, dag=dag
)

# Task 5: Data Quality Check
data_quality_task = PostgresOperator(
    task_id="data_quality_check",
    postgres_conn_id="postgres_default",
    sql="""
        -- 🔍 Data Quality Checks for Recent Yahoo Finance Data
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
                -- Check for data integrity issues
                COUNT(CASE WHEN high_price < low_price THEN 1 END) as invalid_high_low,
                COUNT(CASE WHEN open_price < 0 OR close_price < 0 THEN 1 END) as negative_prices,
                COUNT(CASE WHEN volume < 0 THEN 1 END) as negative_volume
            FROM stock_data 
            WHERE date_recorded >= CURRENT_DATE - INTERVAL '45 days'  -- Last 45 days
            GROUP BY symbol
        )
        SELECT 
            symbol,
            total_records,
            null_opens + null_closes + null_highs + null_lows as total_nulls,
            invalid_high_low,
            negative_prices,
            negative_volume,
            earliest_date,
            latest_date,
            CASE 
                WHEN null_opens + null_closes + null_highs + null_lows > 0 
                     THEN '⚠️ WARNING: NULL price values detected'
                WHEN invalid_high_low > 0 
                     THEN '❌ ERROR: Invalid high/low price relationships'
                WHEN negative_prices > 0 
                     THEN '❌ ERROR: Negative prices detected'
                WHEN negative_volume > 0 
                     THEN '⚠️ WARNING: Negative volume detected'
                ELSE '✅ PASS'
            END as quality_status
        FROM quality_metrics
        ORDER BY symbol;
    """,
    dag=dag,
)

# Task 6: Data Freshness Validation
freshness_task = PythonOperator(
    task_id="validate_data_freshness", python_callable=validate_data_freshness, dag=dag
)

# Define Task Dependencies


# Parallel checks → Data fetch → Parallel validations
(
    [check_api_task, check_db_task]
    >> fetch_data_task
    >> [summary_task, data_quality_task, freshness_task]
)
