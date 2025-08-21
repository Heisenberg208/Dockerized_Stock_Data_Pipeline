# Dockerized Stock Data Pipeline with Airflow

This project implements a robust, scalable data pipeline that automatically fetches stock market data from the Alpha Vantage API and stores it in a PostgreSQL database using Apache Airflow for orchestration.

## 🏗️ Architecture Overview

The pipeline consists of:
- **Apache Airflow**: Orchestrates the data pipeline with scheduling and monitoring
- **PostgreSQL**: Stores historical stock data
- **Redis**: Message broker for Airflow's Celery executor
- **Python Scripts**: Handle API interactions and data processing
- **Docker**: Containerizes the entire application for easy deployment

## 📋 Prerequisites

Before you begin, ensure you have:

1. **Docker and Docker Compose installed**
   - [Install Docker](https://docs.docker.com/get-docker/)
   - [Install Docker Compose](https://docs.docker.com/compose/install/)

2. **Alpha Vantage API Key** (Free)
   - Sign up at [Alpha Vantage](https://www.alphavantage.co/support/#api-key)
   - Get your free API key

## 🚀 Quick Start

### 1. Clone and Setup

```bash
# Create project directory
mkdir stock-pipeline
cd stock-pipeline

# Create the required directory structure
mkdir -p dags scripts

# Copy all the files from the artifacts into their respective locations
```

### 2. Get Your API Key

1. Visit [Alpha Vantage](https://www.alphavantage.co/support/#api-key)
2. Sign up for a free account
3. Copy your API key

### 3. Configure Environment

Edit the `docker-compose.yml` file and replace `YOUR_API_KEY_HERE` with your actual Alpha Vantage API key:

```yaml
- ALPHA_VANTAGE_API_KEY=YOUR_ACTUAL_API_KEY_HERE
```

### 4. Build and Start the Pipeline

```bash
# Build and start all services
docker-compose up -d

# Wait for services to be healthy (about 2-3 minutes)
docker-compose logs -f airflow-init
```

### 5. Access the Airflow Web UI

1. Open your browser to [http://localhost:8080](http://localhost:8080)
2. Login with:
   - **Username**: `admin`
   - **Password**: `admin`

### 6. Activate the Pipeline

1. Find the `stock_data_pipeline` DAG in the Airflow UI
2. Toggle it ON using the switch
3. Click "Trigger DAG" to run it immediately

## 📁 Project Structure

```
stock-pipeline/
├── docker-compose.yml      # Main orchestration file
├── Dockerfile             # Custom Airflow image
├── requirements.txt       # Python dependencies
├── init.sql              # Database schema
├── README.md             # This file
├── dags/
│   └── stock_data_pipeline.py  # Airflow DAG definition
└── scripts/
    └── stock_fetcher.py   # Main data fetching logic
```

## 🔧 How It Works

### Pipeline Steps

1. **API Connection Check**: Verifies Alpha Vantage API connectivity
2. **Database Check**: Ensures PostgreSQL connection and table existence
3. **Data Fetching**: Retrieves stock data for multiple symbols (AAPL, GOOGL, MSFT, TSLA, AMZN)
4. **Data Processing**: Parses JSON response and validates data quality
5. **Database Update**: Uses UPSERT logic to insert new records or update existing ones
6. **Quality Check**: Validates data integrity and completeness
7. **Summary Report**: Generates execution summary

### Data Flow

```
Alpha Vantage API → Python Script → PostgreSQL Database
                ↓
        Airflow Orchestration
                ↓
    Monitoring & Error Handling
```

### Database Schema

The pipeline creates a `stock_data` table with the following structure:

```sql
CREATE TABLE stock_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date_recorded DATE NOT NULL,
    open_price DECIMAL(10, 2),
    high_price DECIMAL(10, 2),
    low_price DECIMAL(10, 2),
    close_price DECIMAL(10, 2),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date_recorded)
);
```

## 🛠️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ALPHA_VANTAGE_API_KEY` | Your Alpha Vantage API key | **Required** |
| `POSTGRES_HOST` | Database hostname | `postgres` |
| `POSTGRES_DB` | Database name | `stockdata` |
| `POSTGRES_USER` | Database username | `airflow` |
| `POSTGRES_PASSWORD` | Database password | `airflow123` |

### Scheduling

The pipeline runs every hour by default. To change this, modify the `schedule_interval` in `dags/stock_data_pipeline.py`:

```python
schedule_interval=timedelta(hours=1),  # Change to your preference
```

### Adding More Stocks

To fetch data for additional symbols, edit the `symbols` list in `dags/stock_data_pipeline.py`:

```python
symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'NVDA', 'META']
```

## 📊 Monitoring and Logs

### Airflow UI Features

- **DAG Graph**: Visual representation of pipeline tasks
- **Task Logs**: Detailed execution logs for debugging
- **Gantt Chart**: Task execution timeline
- **Task Duration**: Historical performance metrics

### Checking Logs

```bash
# View all service logs
docker-compose logs

# View specific service logs
docker-compose logs airflow-worker
docker-compose logs postgres

# Follow logs in real-time
docker-compose logs -f airflow-scheduler
```

### Database Access

Connect to PostgreSQL directly:

```bash
# Access database container
docker-compose exec postgres psql -U airflow -d stockdata

# View recent data
SELECT * FROM stock_data ORDER BY date_recorded DESC LIMIT 10;

# Check data by symbol
SELECT symbol, COUNT(*) as records, MAX(date_recorded) as latest 
FROM stock_data GROUP BY symbol;
```

## 🔍 Troubleshooting

### Common Issues

#### 1. API Key Issues
```
Error: "ALPHA_VANTAGE_API_KEY environment variable is required"
```
**Solution**: Ensure you've replaced `YOUR_API_KEY_HERE` with your actual API key in `docker-compose.yml`

#### 2. Services Not Starting
```bash
# Check service status
docker-compose ps

# Restart services
docker-compose restart

# Rebuild if needed
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

#### 3. Database Connection Issues
```bash
# Check if PostgreSQL is ready
docker-compose exec postgres pg_isready -U airflow

# Reset database
docker-compose down -v  # WARNING: This deletes all data
docker-compose up -d
```

#### 4. Airflow Web UI Not Loading
- Wait 2-3 minutes for all services to initialize
- Check if port 8080 is available: `netstat -an | grep 8080`
- Try restarting: `docker-compose restart airflow-webserver`

### Debug Mode

Enable debug logging by adding to your environment variables:

```yaml
- AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
```

## 🚀 Advanced Usage

### Manual Pipeline Execution

Run the stock fetcher directly:

```bash
# Execute for default symbols
docker-compose exec airflow-worker python /opt/airflow/scripts/stock_fetcher.py

# Execute for specific symbols
docker-compose exec airflow-worker python /opt/airflow/scripts/stock_fetcher.py AAPL MSFT
```

### Scaling Up

The pipeline is designed for scalability:

1. **Add More Workers**: Increase worker replicas in `docker-compose.yml`
2. **Horizontal Scaling**: Deploy across multiple machines
3. **Database Optimization**: Add indexes for frequently queried columns

### Custom Modifications

#### Change API Provider
Modify `scripts/stock_fetcher.py` to use different APIs:
- Yahoo Finance
- IEX Cloud
- Polygon.io

#### Add Data Transformations
Extend the pipeline with:
- Technical indicators (moving averages, RSI)
- Data validation rules
- Real-time alerts

## 🔒 Security Best Practices

1. **Environment Variables**: Never commit API keys to version control
2. **Database Security**: Change default passwords in production
3. **Network Security**: Use Docker networks to isolate services
4. **Updates**: Regularly update Docker images for security patches

## 📝 API Limits

Alpha Vantage free tier limitations:
- **5 API calls per minute**
- **500 API calls per day**

The pipeline handles these limits with:
- Exponential backoff retry logic
- Error handling for rate limits
- Configurable request delays

## 🧪 Testing

### Unit Tests

```bash
# Run tests (you can add pytest later)
docker-compose exec airflow-worker python -m pytest tests/
```

### Data Validation

The pipeline includes automatic data quality checks:
- Null value detection
- Date range validation
- Price consistency checks
- Volume anomaly detection

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🆘 Support

If you encounter issues:

1. Check the troubleshooting section above
2. Review the logs: `docker-compose logs`
3. Ensure all prerequisites are met
4. Verify your API key is valid

## 🎯 Next Steps

Once you have the basic pipeline running, consider:

1. **Adding more data sources** (news, financial reports)
2. **Implementing data analysis** (trends, predictions)
3. **Creating dashboards** (Grafana, Tableau)
4. **Setting up alerts** (price thresholds, volume spikes)
5. **Deploying to cloud** (AWS, GCP, Azure)

---

**Happy Trading! 📈**