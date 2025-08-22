from datetime import datetime, timedelta
import os
import sys
import requests
import json
import psycopg2
import time
import logging
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class YahooStockDataFetcher:
    """
    A class to fetch stock data from Yahoo Finance API and store it in PostgreSQL
    """

    def __init__(self):
        self.db_config = {
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "database": os.getenv("POSTGRES_DB", "stockdata"),
            "user": os.getenv("POSTGRES_USER", "airflow"),
            "password": os.getenv("POSTGRES_PASSWORD", "airflow123"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
        }

        # Yahoo Finance
        self.base_url = "https://query1.finance.yahoo.com/v8/finance/chart"

        # Headers to mimic browser request
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

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
                time.sleep(2)

    def fetch_stock_data(
        self, symbol: str = "AAPL", period: str = "3mo", interval: str = "1d"
    ) -> Optional[Dict]:
        """
        Fetch stock data from Yahoo Finance API

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            period: Time period ('1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
        """
        try:
            # Yahoo Finance API endpoint
            url = f"{self.base_url}/{symbol}"

            params = {
                "range": period,
                "interval": interval,
                "includePrePost": "true",
                "events": "div%2Csplit",
            }

            logger.info(f"Fetching stock data for {symbol} (period: {period})")
            response = requests.get(
                url, params=params, headers=self.headers, timeout=30
            )
            response.raise_for_status()

            data = response.json()

            # Check for API errors
            if "chart" not in data or not data["chart"]["result"]:
                logger.error(f"No data returned for {symbol}")
                return None

            chart_data = data["chart"]["result"][0]

            if "timestamp" not in chart_data or not chart_data["timestamp"]:
                logger.error(f"No timestamp data for {symbol}")
                return None

            logger.info(f"Successfully fetched data for {symbol}")
            return chart_data

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {symbol}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching data for {symbol}: {e}")
            return None

    def parse_stock_data(self, data: Dict, symbol: str) -> List[Dict]:
        """
        Parse the Yahoo Finance API response and extract relevant data points
        """
        try:
            timestamps = data.get("timestamp", [])
            indicators = data.get("indicators", {})
            quote = indicators.get("quote", [{}])[0]

            # Extract price arrays
            opens = quote.get("open", [])
            highs = quote.get("high", [])
            lows = quote.get("low", [])
            closes = quote.get("close", [])
            volumes = quote.get("volume", [])

            parsed_data = []

            for i, timestamp in enumerate(timestamps):
                try:
                    # Convert timestamp to date
                    date_obj = datetime.fromtimestamp(timestamp)

                    # Skip if any of the essential price data is missing
                    if (
                        i >= len(opens)
                        or i >= len(highs)
                        or i >= len(lows)
                        or i >= len(closes)
                    ):
                        continue

                    # Extract price data with proper error handling
                    record = {
                        "symbol": symbol,
                        "date_recorded": date_obj,
                        "open_price": self._safe_float_conversion(opens[i]),
                        "high_price": self._safe_float_conversion(highs[i]),
                        "low_price": self._safe_float_conversion(lows[i]),
                        "close_price": self._safe_float_conversion(closes[i]),
                        "volume": self._safe_int_conversion(
                            volumes[i] if i < len(volumes) else None
                        ),
                    }

                    # Only add record if we have essential price data
                    if (
                        record["open_price"] is not None
                        and record["high_price"] is not None
                        and record["low_price"] is not None
                        and record["close_price"] is not None
                    ):
                        parsed_data.append(record)

                except (ValueError, IndexError) as e:
                    logger.warning(f"Skipping invalid data point {i} for {symbol}: {e}")
                    continue

            logger.info(f"Parsed {len(parsed_data)} records for {symbol}")
            return parsed_data

        except Exception as e:
            logger.error(f"Error parsing stock data for {symbol}: {e}")
            return []

    def _safe_float_conversion(self, value) -> Optional[float]:
        """Safely convert value to float"""
        try:
            if value is None:
                return None
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int_conversion(self, value) -> Optional[int]:
        """Safely convert value to int"""
        try:
            if value is None:
                return None
            return int(float(value))
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

            logger.info(
                f"Successfully updated {len(stock_records)} records in database"
            )
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

    def run_pipeline(
        self, symbols: List[str] = None, period: str = "3mo", interval: str = "1d"
    ) -> bool:
        """
        Run the complete data pipeline for specified symbols

        Args:
            symbols: List of stock symbols
            period: Time period for data ('1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
        """
        if symbols is None:
            symbols = ["AAPL", "GOOGL", "MSFT", "TSLA"]

        logger.info(f"Starting pipeline for symbols: {symbols} (period: {period})")
        overall_success = True

        for symbol in symbols:
            try:
                # Fetch data from Yahoo Finance
                api_data = self.fetch_stock_data(symbol, period, interval)

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

                # Small delay to handle rate limits
                time.sleep(1)

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
        # Parse command line arguments
        symbols = []
        period = "3mo"  # Default period

        # Simple argument parsing
        args = sys.argv[1:]
        i = 0
        while i < len(args):
            if args[i] == "--period" and i + 1 < len(args):
                period = args[i + 1]
                i += 2
            else:
                symbols.append(args[i])
                i += 1

        # Use defaults if no symbols provided
        if not symbols:
            symbols = ["AAPL", "GOOGL", "MSFT", "TSLA"]

        logger.info(f"Running for symbols: {symbols}, period: {period}")

        # Initialize and run the fetcher
        fetcher = YahooStockDataFetcher()
        success = fetcher.run_pipeline(symbols, period)

        # Exit with appropriate code
        sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
