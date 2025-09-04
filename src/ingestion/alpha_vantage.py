"""
Alpha Vantage data ingestion service.

This module provides data ingestion capabilities from Alpha Vantage API
for real-time and historical financial market data.
"""

import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import structlog

from src.ingestion.base import BaseDataIngestion, IngestionResult, DataSource
from src.models.market_data import TickData, OHLCVData
from src.config.settings import settings


class AlphaVantageIngestion(BaseDataIngestion):
    """
    Alpha Vantage data ingestion service.
    
    Provides real-time and historical market data from Alpha Vantage API
    with proper rate limiting and error handling.
    """
    
    def __init__(self, symbols: List[str], api_key: Optional[str] = None):
        """
        Initialize Alpha Vantage ingestion service.
        
        Args:
            symbols: List of symbols to ingest
            api_key: Alpha Vantage API key (optional, can be set via environment)
        """
        super().__init__(symbols)
        self.logger = structlog.get_logger("AlphaVantageIngestion")
        self.api_key = api_key or getattr(settings.api, 'alpha_vantage_api_key', None)
        self.base_url = "https://www.alphavantage.co/query"
        self.rate_limit_delay = 12.0  # Alpha Vantage free tier: 5 calls per minute
        
        if not self.api_key:
            self.logger.warning("Alpha Vantage API key not provided. Service will be disabled.")
    
    async def fetch_data(self, symbol: str) -> IngestionResult:
        """
        Fetch real-time data for a symbol from Alpha Vantage.
        
        Args:
            symbol: Stock symbol to fetch data for
            
        Returns:
            IngestionResult containing the fetched data
        """
        if not self.api_key:
            return IngestionResult(
                success=False,
                data=None,
                error="Alpha Vantage API key not configured",
                source=DataSource.ALPHA_VANTAGE
            )
        
        try:
            # Fetch real-time quote
            quote_data = await self._fetch_quote(symbol)
            
            if quote_data and 'Global Quote' in quote_data:
                global_quote = quote_data['Global Quote']
                
                # Convert to TickData
                tick_data = TickData(
                    symbol=symbol,
                    price=float(global_quote.get('05. price', 0.0)),
                    volume=int(global_quote.get('06. volume', 0)),
                    timestamp=datetime.utcnow(),
                    source=DataSource.ALPHA_VANTAGE,
                    bid=float(global_quote.get('03. low', 0.0)),
                    ask=float(global_quote.get('02. high', 0.0))
                )
                
                return IngestionResult(
                    success=True,
                    data=tick_data,
                    error=None,
                    source=DataSource.ALPHA_VANTAGE
                )
            else:
                return IngestionResult(
                    success=False,
                    data=None,
                    error="No data received from Alpha Vantage API",
                    source=DataSource.ALPHA_VANTAGE
                )
                
        except Exception as e:
            self.logger.error("Failed to fetch data from Alpha Vantage", 
                            symbol=symbol, 
                            error=str(e))
            return IngestionResult(
                success=False,
                data=None,
                error=str(e),
                source=DataSource.ALPHA_VANTAGE
            )
    
    async def fetch_historical_data(self, symbol: str, days: int = 30) -> IngestionResult:
        """
        Fetch historical OHLCV data from Alpha Vantage.
        
        Args:
            symbol: Stock symbol to fetch data for
            days: Number of days of historical data
            
        Returns:
            IngestionResult containing historical data
        """
        if not self.api_key:
            return IngestionResult(
                success=False,
                data=None,
                error="Alpha Vantage API key not configured",
                source=DataSource.ALPHA_VANTAGE
            )
        
        try:
            # Fetch daily time series
            time_series_data = await self._fetch_time_series(symbol)
            
            if time_series_data and 'Time Series (Daily)' in time_series_data:
                time_series = time_series_data['Time Series (Daily)']
                ohlcv_list = []
                
                # Convert to OHLCVData objects
                for date_str, data in list(time_series.items())[:days]:
                    try:
                        ohlcv_data = OHLCVData(
                            symbol=symbol,
                            timestamp=datetime.strptime(date_str, '%Y-%m-%d'),
                            open_price=float(data['1. open']),
                            high_price=float(data['2. high']),
                            low_price=float(data['3. low']),
                            close_price=float(data['4. close']),
                            volume=int(data['5. volume']),
                            period='1D',
                            source=DataSource.ALPHA_VANTAGE
                        )
                        ohlcv_list.append(ohlcv_data)
                    except (ValueError, KeyError) as e:
                        self.logger.warning("Failed to parse OHLCV data", 
                                          date=date_str, 
                                          error=str(e))
                        continue
                
                return IngestionResult(
                    success=True,
                    data=ohlcv_list,
                    error=None,
                    source=DataSource.ALPHA_VANTAGE
                )
            else:
                return IngestionResult(
                    success=False,
                    data=None,
                    error="No historical data received from Alpha Vantage API",
                    source=DataSource.ALPHA_VANTAGE
                )
                
        except Exception as e:
            self.logger.error("Failed to fetch historical data from Alpha Vantage", 
                            symbol=symbol, 
                            error=str(e))
            return IngestionResult(
                success=False,
                data=None,
                error=str(e),
                source=DataSource.ALPHA_VANTAGE
            )
    
    async def _fetch_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch real-time quote from Alpha Vantage."""
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    self.logger.error("Alpha Vantage API error", 
                                    status=response.status,
                                    symbol=symbol)
                    return None
    
    async def _fetch_time_series(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch daily time series from Alpha Vantage."""
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_key,
            "outputsize": "compact"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    self.logger.error("Alpha Vantage API error", 
                                    status=response.status,
                                    symbol=symbol)
                    return None
    
    async def health_check(self) -> bool:
        """
        Check if Alpha Vantage service is healthy.
        
        Returns:
            True if service is healthy, False otherwise
        """
        if not self.api_key:
            return False
        
        try:
            # Simple API call to check connectivity
            params = {
                "function": "GLOBAL_QUOTE",
                "symbol": "AAPL",
                "apikey": self.api_key
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params, timeout=10) as response:
                    return response.status == 200
        except Exception as e:
            self.logger.error("Alpha Vantage health check failed", error=str(e))
            return False
