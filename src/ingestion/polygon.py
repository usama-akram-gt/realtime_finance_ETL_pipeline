"""
Polygon.io data ingestion service.

This module provides data ingestion capabilities from Polygon.io API
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


class PolygonIngestion(BaseDataIngestion):
    """
    Polygon.io data ingestion service.
    
    Provides real-time and historical market data from Polygon.io API
    with proper rate limiting and error handling.
    """
    
    def __init__(self, symbols: List[str], api_key: Optional[str] = None):
        """
        Initialize Polygon ingestion service.
        
        Args:
            symbols: List of symbols to ingest
            api_key: Polygon.io API key (optional, can be set via environment)
        """
        super().__init__(symbols)
        self.logger = structlog.get_logger("PolygonIngestion")
        self.api_key = api_key or getattr(settings.api, 'polygon_api_key', None)
        self.base_url = "https://api.polygon.io"
        self.rate_limit_delay = 0.1  # 100ms between requests
        
        if not self.api_key:
            self.logger.warning("Polygon API key not provided. Service will be disabled.")
    
    async def fetch_data(self, symbol: str) -> IngestionResult:
        """
        Fetch real-time data for a symbol from Polygon.io.
        
        Args:
            symbol: Stock symbol to fetch data for
            
        Returns:
            IngestionResult containing the fetched data
        """
        if not self.api_key:
            return IngestionResult(
                success=False,
                data=None,
                error="Polygon API key not configured",
                source=DataSource.POLYGON
            )
        
        try:
            # Fetch real-time quote
            quote_data = await self._fetch_quote(symbol)
            
            if quote_data:
                # Convert to TickData
                tick_data = TickData(
                    symbol=symbol,
                    price=quote_data.get('p', 0.0),
                    volume=quote_data.get('s', 0),
                    timestamp=datetime.utcnow(),
                    source=DataSource.POLYGON,
                    bid=quote_data.get('bp', 0.0),
                    ask=quote_data.get('ap', 0.0),
                    bid_size=quote_data.get('bs', 0),
                    ask_size=quote_data.get('as', 0)
                )
                
                return IngestionResult(
                    success=True,
                    data=tick_data,
                    error=None,
                    source=DataSource.POLYGON
                )
            else:
                return IngestionResult(
                    success=False,
                    data=None,
                    error="No data received from Polygon API",
                    source=DataSource.POLYGON
                )
                
        except Exception as e:
            self.logger.error("Failed to fetch data from Polygon", 
                            symbol=symbol, 
                            error=str(e))
            return IngestionResult(
                success=False,
                data=None,
                error=str(e),
                source=DataSource.POLYGON
            )
    
    async def fetch_historical_data(self, symbol: str, days: int = 30) -> IngestionResult:
        """
        Fetch historical OHLCV data from Polygon.io.
        
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
                error="Polygon API key not configured",
                source=DataSource.POLYGON
            )
        
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Fetch historical bars
            bars_data = await self._fetch_bars(symbol, start_date, end_date)
            
            if bars_data and 'results' in bars_data:
                ohlcv_list = []
                for bar in bars_data['results']:
                    ohlcv_data = OHLCVData(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(bar['t'] / 1000),
                        open_price=bar['o'],
                        high_price=bar['h'],
                        low_price=bar['l'],
                        close_price=bar['c'],
                        volume=bar['v'],
                        period='1D',
                        source=DataSource.POLYGON
                    )
                    ohlcv_list.append(ohlcv_data)
                
                return IngestionResult(
                    success=True,
                    data=ohlcv_list,
                    error=None,
                    source=DataSource.POLYGON
                )
            else:
                return IngestionResult(
                    success=False,
                    data=None,
                    error="No historical data received from Polygon API",
                    source=DataSource.POLYGON
                )
                
        except Exception as e:
            self.logger.error("Failed to fetch historical data from Polygon", 
                            symbol=symbol, 
                            error=str(e))
            return IngestionResult(
                success=False,
                data=None,
                error=str(e),
                source=DataSource.POLYGON
            )
    
    async def _fetch_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch real-time quote from Polygon.io."""
        url = f"{self.base_url}/v2/last/trade/{symbol}"
        params = {"apikey": self.api_key}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('results', {})
                else:
                    self.logger.error("Polygon API error", 
                                    status=response.status,
                                    symbol=symbol)
                    return None
    
    async def _fetch_bars(self, symbol: str, start_date: datetime, end_date: datetime) -> Optional[Dict[str, Any]]:
        """Fetch historical bars from Polygon.io."""
        url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
        params = {"apikey": self.api_key}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    self.logger.error("Polygon API error", 
                                    status=response.status,
                                    symbol=symbol)
                    return None
    
    async def health_check(self) -> bool:
        """
        Check if Polygon.io service is healthy.
        
        Returns:
            True if service is healthy, False otherwise
        """
        if not self.api_key:
            return False
        
        try:
            # Simple API call to check connectivity
            url = f"{self.base_url}/v2/aggs/ticker/AAPL/prev"
            params = {"apikey": self.api_key}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=5) as response:
                    return response.status == 200
        except Exception as e:
            self.logger.error("Polygon health check failed", error=str(e))
            return False
