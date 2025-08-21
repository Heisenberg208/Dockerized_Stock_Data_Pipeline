import os
import sys
import requests
import json
import psycopg2
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockDataFetcher:
    """
    A class to fetch stock data from Alpha Vantage API and store it in PostgreSQL
    """
    
    def __init__(self):
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'database': os.getenv('POSTGRES_DB', 'stockdata'),
            'user': os.getenv('POSTGRES_USER', 'airflow'),
            'password': os.getenv('POSTGRES_PASSWORD', 'airflow123'),
            'port': os.getenv('POSTGRES_PORT', '5432')
        }
        
        # Validate required environment variables
        if not self.api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required")
        
        self.base_url = "https://www.alphavantage.co/query"
        
    def get_database_connection(self):
        """
        Establish and return a database connection with retry logic
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                conn = psycopg2.connect(**self.db_config)
                logger.info("Successfully connected to PostgreSQL database")
                return conn
            except psycopg2.Error as e:
                retry_count += 1
                logger.error(f"Database connection attempt {retry_count} failed: {e}")
                if retry_count >= max_retries:
                    raise
                time.sleep(5)  # Wait before retrying
    
    def fetch_stock_data(self, symbol: str = "AAPL") -> Optional[Dict]:
        """
        Fetch daily stock data from Alpha Vantage API
        """
        try:
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol,
                'apikey': self.api_key,
                'outputsize': 'compact'  # Get last 100 data points
            }
            
            logger.info(f"Fetching stock data for {symbol}")
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                logger.error(f"API Error: {data['Error Message']}")
                return None
            
            if 'Note' in data:
                logger.warning(f"API Note: {data['Note']}")
                return None
            
            if 'Time Series (Daily)' not in data:
                logger.error(f"Unexpected API response format: {list(data.keys())}")
                return None
            
            logger.info(f"Successfully fetched data for {symbol}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching data: {e}")
            return None
    
    def parse_stock_data(self, data: Dict, symbol: str) -> List[Dict]:
        """
        Parse the API response and extract relevant data points
        """
        try:
            time_series = data.get('Time Series (Daily)', {})
            parsed_data = []
            
            for date_str, daily_data in time_series.items():
                try:
                    # Parse the date
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                    
                    # Extract price data with proper error handling
                    record = {
                        'symbol': symbol,
                        'date_recorded': date_obj,
                        'open_price': self._safe_float_conversion(daily_data.get('1. open')),
                        'high_price': self._safe_float_conversion(daily_data.get('2. high')),
                        'low_price': self._safe_float_conversion(daily_data.get('3. low')),
                        'close_price': self._safe_float_conversion(daily_data.get('4. close')),
                        'volume': self._safe_int_conversion(daily_data.get('5. volume'))
                    }
                    
                    parsed_data.append(record)
                    
                except ValueError as e:
                    logger.warning(f"Skipping invalid date entry {date_str}: {e}")
                    continue
            
            logger.info(f"Parsed {len(parsed_data)} records for {symbol}")
            return parsed_data
            
        except Exception as e:
            logger.error(f"Error parsing stock data: {e}")
            return []
    
    def _safe_float_conversion(self, value: str) -> Optional[float]:
        """Safely convert string to float"""
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None
    
    def _safe_int_conversion(self, value: str) -> Optional[int]:
        """Safely convert string to int"""
        try:
            return int(float(value)) if value else None
        except (ValueError, TypeError):
            return None
    
    def update_database(self, stock_records: List[Dict]) -> bool:
        """
        Update PostgreSQL database with stock data using UPSERT logic
        """
        if not stock_records:
            logger.warning("No stock records to update")
            return False
        
        conn = None
        cursor = None
        
        try:
            conn = self.get_database_connection()
            cursor = conn.cursor()
            
            # Prepare the UPSERT query (INSERT ... ON CONFLICT)
            upsert_query = """
                INSERT INTO stock_data (symbol, date_recorded, open_price, high_price, low_price, close_price, volume, updated_at)
                VALUES (%(symbol)s, %(date_recorded)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s, %(volume)s, CURRENT_TIMESTAMP)
                ON CONFLICT (symbol, date_recorded)
                DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    updated_at = CURRENT_TIMESTAMP;
            """
            
            # Execute the batch insert/update
            cursor.executemany(upsert_query, stock_records)
            conn.commit()
            
            logger.info(f"Successfully updated {len(stock_records)} records in database")
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Database error: {e}")
            if conn:
                conn.rollback()
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating database: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def run_pipeline(self, symbols: List[str] = None) -> bool:
        """
        Run the complete data pipeline for specified symbols
        """
        if symbols is None:
            symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']  # Default symbols
        
        logger.info(f"Starting pipeline for symbols: {symbols}")
        overall_success = True
        
        for symbol in symbols:
            try:
                # Fetch data from API
                api_data = self.fetch_stock_data(symbol)
                
                if api_data is None:
                    logger.error(f"Failed to fetch data for {symbol}")
                    overall_success = False
                    continue
                
                # Parse the data
                parsed_data = self.parse_stock_data(api_data, symbol)
                
                if not parsed_data:
                    logger.error(f"No valid data parsed for {symbol}")
                    overall_success = False
                    continue
                
                # Update database
                update_success = self.update_database(parsed_data)
                
                if not update_success:
                    logger.error(f"Failed to update database for {symbol}")
                    overall_success = False
                
            except Exception as e:
                logger.error(f"Pipeline failed for {symbol}: {e}")
                overall_success = False
        
        if overall_success:
            logger.info("Pipeline completed successfully for all symbols")
        else:
            logger.warning("Pipeline completed with some errors")
        
        return overall_success

def main():
    """
    Main function to run the stock data fetcher
    """
    try:
        # Get symbols from command line arguments or use defaults
        symbols = sys.argv[1:] if len(sys.argv) > 1 else ['AAPL', 'GOOGL', 'MSFT', 'TSLA']
        
        # Initialize and run the fetcher
        fetcher = StockDataFetcher()
        success = fetcher.run_pipeline(symbols)
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()