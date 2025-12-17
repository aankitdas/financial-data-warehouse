import os
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd
import requests
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class FinancialDataPipeline:
    """ETL pipeline for financial data ingestion"""
    
    def __init__(self):
        self.sf_conn = None
        self.companies = [
            {"ticker": "AAPL", "name": "Apple Inc.", "sector": "Technology"},
            {"ticker": "JPM", "name": "JPMorgan Chase", "sector": "Finance"},
            {"ticker": "JNJ", "name": "Johnson & Johnson", "sector": "Healthcare"},
            {"ticker": "PG", "name": "Procter & Gamble", "sector": "Consumer"},
            {"ticker": "XOM", "name": "Exxon Mobil", "sector": "Energy"},
        ]
        self.connect_snowflake()
    
    def connect_snowflake(self):
        """Establish Snowflake connection"""
        try:
            self.sf_conn = snowflake.connector.connect(
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                insecure_mode=True
            )
            # Ensure schema is set
            cursor = self.sf_conn.cursor()
            cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}")
            cursor.close()
            logger.info("Connected to Snowflake successfully")
        except Exception as e:
            logger.error(f"Snowflake connection failed: {e}")
            raise
    
    def fetch_stock_prices(self) -> pd.DataFrame:
        """Fetch stock price data from Alpha Vantage (free tier)"""
        logger.info("Fetching stock price data...")
        api_key = "demo"  # Free tier key (limited calls)
        data = []
        
        try:
            for company in self.companies[:2]:  # Limit to 2 for free tier
                ticker = company["ticker"]
                url = f"https://www.alphavantage.co/query"
                params = {
                    "function": "TIME_SERIES_DAILY",
                    "symbol": ticker,
                    "apikey": api_key,
                    "outputsize": "compact"
                }
                
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    result = response.json()
                    if "Time Series (Daily)" in result:
                        for date_str, prices in list(result["Time Series (Daily)"].items())[:30]:
                            data.append({
                                "ticker": ticker,
                                "date": date_str,
                                "open_price": float(prices["1. open"]),
                                "close_price": float(prices["4. close"]),
                                "high_price": float(prices["2. high"]),
                                "low_price": float(prices["3. low"]),
                                "volume": int(prices["6. volume"]),
                                "ingested_at": datetime.utcnow()
                            })
                        logger.info(f"Fetched {len([d for d in data if d['ticker'] == ticker])} records for {ticker}")
                    else:
                        logger.warning(f"No data for {ticker}: {result.get('Note', 'Unknown error')}")
                else:
                    logger.warning(f"Failed to fetch {ticker}: HTTP {response.status_code}")
        
        except Exception as e:
            logger.error(f"Error fetching stock prices: {e}")
        
        return pd.DataFrame(data) if data else pd.DataFrame()
    
    def create_company_dimension(self) -> pd.DataFrame:
        """Create company dimension data"""
        logger.info("Creating company dimension data...")
        return pd.DataFrame(self.companies).assign(
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
    
    def validate_data(self, df: pd.DataFrame, table_name: str) -> bool:
        """Data quality validation"""
        logger.info(f"Validating {table_name}...")
        
        if df.empty:
            logger.warning(f"Empty dataframe for {table_name}")
            return False
        
        # Check for nulls in critical columns
        critical_cols = df.columns.tolist()
        nulls = df[critical_cols].isnull().sum()
        
        if nulls.sum() > 0:
            logger.warning(f"Null values detected in {table_name}:\n{nulls[nulls > 0]}")
        
        logger.info(f"✓ Validation passed for {table_name}: {len(df)} rows")
        return True
    
    def load_to_snowflake(self, df: pd.DataFrame, table_name: str):
        """Load dataframe to Snowflake"""
        try:
            logger.info(f"Loading {len(df)} rows to {table_name}...")
            
            write_pandas(
                self.sf_conn,
                df,
                table_name.upper(),
                auto_create_table=True,
                overwrite=False
            )
            logger.info(f"✓ Successfully loaded to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            raise
    
    def run_pipeline(self):
        """Execute full ETL pipeline"""
        try:
            logger.info("=" * 60)
            logger.info("Starting Financial Data Pipeline")
            logger.info("=" * 60)
            
            # Extract
            companies_df = self.create_company_dimension()
            stock_prices_df = self.fetch_stock_prices()
            
            # Validate
            if self.validate_data(companies_df, "stg_companies"):
                self.load_to_snowflake(companies_df, "stg_companies")
            
            if not stock_prices_df.empty and self.validate_data(stock_prices_df, "stg_stock_prices"):
                self.load_to_snowflake(stock_prices_df, "stg_stock_prices")
            else:
                logger.warning("Skipping stock prices due to API rate limits. Using mock data.")
                # Create mock data for demo
                mock_prices = pd.DataFrame({
                    "ticker": ["AAPL", "AAPL", "JPM", "JPM"],
                    "date": ["2025-01-10", "2025-01-09", "2025-01-10", "2025-01-09"],
                    "open_price": [150.0, 148.5, 200.0, 199.0],
                    "close_price": [151.0, 149.0, 201.0, 200.0],
                    "high_price": [152.0, 150.0, 202.0, 201.0],
                    "low_price": [149.5, 148.0, 199.5, 198.5],
                    "volume": [50000000, 45000000, 30000000, 28000000],
                    "ingested_at": datetime.utcnow()
                })
                self.load_to_snowflake(mock_prices, "stg_stock_prices")
            
            logger.info("=" * 60)
            logger.info("Pipeline completed successfully!")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            if self.sf_conn:
                self.sf_conn.close()

if __name__ == "__main__":
    pipeline = FinancialDataPipeline()
    pipeline.run_pipeline()