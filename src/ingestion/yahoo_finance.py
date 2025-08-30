"""
Yahoo Finance data ingestion service.

This module provides data ingestion from Yahoo Finance API
with proper error handling, rate limiting, and data validation.
"""

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional
import aiohttp
import yfinance as yf

from src.ingestion.base import BaseDataIngestion, IngestionResult
from src.models.market_data import DataSource, TickData, OHLCVData
from src.config.settings import settings


class YahooFinanceIngestion(BaseDataIngestion):
    """
    Yahoo Finance data ingestion service.
    
    Provides real-time and historical market data from Yahoo Finance
    with proper error handling and rate limiting.
    """
    
    def __init__(self, symbols: Optional[list] = None):
        """
        Initialize Yahoo Finance ingestion service.
        
        Args:
            symbols: List of stock symbols to track
        """
        super().__init__(DataSource.YAHOO_FINANCE, symbols)
        self.session: Optional[aiohttp.ClientSession] = None
        self._rate_limit_semaphore = asyncio.Semaphore(settings.api.requests_per_minute)
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session
    
    async def fetch_data(self, symbol: str) -> IngestionResult:
        """
        Fetch real-time data for a symbol from Yahoo Finance.
        
        Args:
            symbol: Stock symbol to fetch data for
            
        Returns:
            IngestionResult with fetched data or error
        """
        try:
            async with self._rate_limit_semaphore:
                # Use yfinance for data fetching
                ticker = yf.Ticker(symbol)
                info = ticker.info
                
                # Get real-time price data
                hist = ticker.history(period="1d", interval="1m")
                
                if hist.empty:
                    return IngestionResult(
                        success=False,
                        error=f"No data available for symbol {symbol}",
                        source=self.source
                    )
                
                # Get latest data point
                latest = hist.iloc[-1]
                
                data = {
                    "symbol": symbol,
                    "price": float(latest["Close"]),
                    "volume": int(latest["Volume"]),
                    "open": float(latest["Open"]),
                    "high": float(latest["High"]),
                    "low": float(latest["Low"]),
                    "close": float(latest["Close"]),
                    "timestamp": latest.name.to_pydatetime(),
                    "info": info
                }
                
                return IngestionResult(
                    success=True,
                    data=data,
                    source=self.source
                )
                
        except Exception as e:
            self.logger.error("Failed to fetch Yahoo Finance data", 
                            symbol=symbol, error=str(e))
            return IngestionResult(
                success=False,
                error=f"Failed to fetch data: {str(e)}",
                source=self.source
            )
    
    async def validate_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate fetched data from Yahoo Finance.
        
        Args:
            data: Raw data to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        try:
            required_fields = ["symbol", "price", "volume", "timestamp"]
            
            # Check required fields
            for field in required_fields:
                if field not in data:
                    self.logger.warning("Missing required field", field=field)
                    return False
            
            # Validate price
            if not isinstance(data["price"], (int, float)) or data["price"] <= 0:
                self.logger.warning("Invalid price", price=data["price"])
                return False
            
            # Validate volume
            if not isinstance(data["volume"], (int, float)) or data["volume"] < 0:
                self.logger.warning("Invalid volume", volume=data["volume"])
                return False
            
            # Validate timestamp
            if not isinstance(data["timestamp"], datetime):
                self.logger.warning("Invalid timestamp", timestamp=data["timestamp"])
                return False
            
            return True
            
        except Exception as e:
            self.logger.error("Data validation error", error=str(e))
            return False
    
    async def transform_data(self, data: Dict[str, Any], symbol: str) -> TickData:
        """
        Transform Yahoo Finance data into standardized format.
        
        Args:
            data: Raw data from Yahoo Finance
            symbol: Stock symbol
            
        Returns:
            Standardized TickData object
        """
        return TickData(
            symbol=data["symbol"],
            timestamp=data["timestamp"],
            price=Decimal(str(data["price"])),
            volume=data["volume"],
            bid=None,  # Yahoo Finance doesn't provide bid/ask in basic API
            ask=None,
            bid_size=None,
            ask_size=None,
            source=self.source,
            exchange="NASDAQ"  # Default, could be enhanced with actual exchange detection
        )
    
    async def fetch_historical_data(self, symbol: str, period: str = "1y", interval: str = "1d") -> Optional[OHLCVData]:
        """
        Fetch historical OHLCV data for a symbol.
        
        Args:
            symbol: Stock symbol
            period: Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
            
        Returns:
            OHLCVData object or None if failed
        """
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period, interval=interval)
            
            if hist.empty:
                self.logger.warning("No historical data available", symbol=symbol, period=period)
                return None
            
            # Get latest data point
            latest = hist.iloc[-1]
            
            return OHLCVData(
                symbol=symbol,
                timestamp=latest.name.to_pydatetime(),
                open_price=Decimal(str(latest["Open"])),
                high_price=Decimal(str(latest["High"])),
                low_price=Decimal(str(latest["Low"])),
                close_price=Decimal(str(latest["Close"])),
                volume=int(latest["Volume"]),
                period=interval,
                source=self.source
            )
            
        except Exception as e:
            self.logger.error("Failed to fetch historical data", 
                            symbol=symbol, period=period, error=str(e))
            return None
    
    async def fetch_company_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch company information for a symbol.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Company information dictionary or None if failed
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # Extract relevant information
            company_info = {
                "symbol": symbol,
                "name": info.get("longName", ""),
                "sector": info.get("sector", ""),
                "industry": info.get("industry", ""),
                "market_cap": info.get("marketCap"),
                "pe_ratio": info.get("trailingPE"),
                "dividend_yield": info.get("dividendYield"),
                "beta": info.get("beta"),
                "fifty_two_week_high": info.get("fiftyTwoWeekHigh"),
                "fifty_two_week_low": info.get("fiftyTwoWeekLow"),
                "volume_avg": info.get("averageVolume"),
                "currency": info.get("currency", "USD"),
                "exchange": info.get("exchange", ""),
                "country": info.get("country", ""),
                "website": info.get("website", ""),
                "description": info.get("longBusinessSummary", "")
            }
            
            return company_info
            
        except Exception as e:
            self.logger.error("Failed to fetch company info", symbol=symbol, error=str(e))
            return None
    
    async def close(self) -> None:
        """Close the ingestion service and cleanup resources."""
        if self.session and not self.session.closed:
            await self.session.close()
        self.logger.info("Yahoo Finance ingestion service closed")


class YahooFinanceBatchIngestion(YahooFinanceIngestion):
    """
    Batch Yahoo Finance data ingestion for multiple symbols.
    
    Optimized for fetching data for multiple symbols in batches
    to reduce API calls and improve performance.
    """
    
    def __init__(self, symbols: Optional[list] = None, batch_size: int = 10):
        """
        Initialize batch ingestion service.
        
        Args:
            symbols: List of stock symbols to track
            batch_size: Number of symbols to process in each batch
        """
        super().__init__(symbols)
        self.batch_size = batch_size
    
    async def fetch_batch_data(self, symbols: list) -> Dict[str, IngestionResult]:
        """
        Fetch data for multiple symbols in a batch.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            Dictionary mapping symbols to IngestionResult
        """
        results = {}
        
        # Process symbols in batches
        for i in range(0, len(symbols), self.batch_size):
            batch = symbols[i:i + self.batch_size]
            
            # Create tasks for batch
            tasks = [self.ingest_single_symbol(symbol) for symbol in batch]
            
            # Execute batch
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for symbol, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    results[symbol] = IngestionResult(
                        success=False,
                        error=str(result),
                        source=self.source
                    )
                else:
                    results[symbol] = result
            
            # Rate limiting between batches
            if i + self.batch_size < len(symbols):
                await asyncio.sleep(60 / settings.api.requests_per_minute)
        
        return results
    
    async def start_batch_ingestion(self, interval: int = None) -> None:
        """
        Start continuous batch ingestion.
        
        Args:
            interval: Interval between ingestion cycles in seconds
        """
        interval = interval or settings.update_interval
        self.is_running = True
        
        self.logger.info("Starting batch ingestion", 
                        interval=interval, 
                        symbols=self.symbols,
                        batch_size=self.batch_size)
        
        while self.is_running:
            try:
                results = await self.fetch_batch_data(self.symbols)
                
                # Log results
                success_count = sum(1 for r in results.values() if r.success)
                error_count = len(results) - success_count
                
                self.logger.info("Batch ingestion completed", 
                               total=len(results),
                               success=success_count,
                               errors=error_count)
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                self.logger.error("Batch ingestion error", error=str(e))
                await asyncio.sleep(interval)
