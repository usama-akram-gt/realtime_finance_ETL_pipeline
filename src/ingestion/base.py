"""
Base classes for data ingestion.

This module provides abstract base classes and common functionality
for all data ingestion services.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import AsyncGenerator, Dict, Any, Optional, List
from dataclasses import dataclass

import structlog
from pydantic import ValidationError

from src.config.settings import settings
from src.models.market_data import DataSource, TickData, OHLCVData, MarketDataEvent
from src.utils.monitoring import MetricsCollector


@dataclass
class IngestionResult:
    """Result of a data ingestion operation."""
    
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: datetime = None
    source: Optional[DataSource] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


class BaseDataIngestion(ABC):
    """
    Abstract base class for data ingestion services.
    
    Provides common functionality for all data ingestion implementations
    including error handling, logging, metrics collection, and retry logic.
    """
    
    def __init__(self, source: DataSource, symbols: Optional[List[str]] = None):
        """
        Initialize the data ingestion service.
        
        Args:
            source: Data source identifier
            symbols: List of symbols to ingest data for
        """
        self.source = source
        self.symbols = symbols or settings.symbols
        self.logger = structlog.get_logger(f"{self.__class__.__name__}")
        self.metrics = MetricsCollector()
        self.is_running = False
        self._retry_count = 0
        self._max_retries = settings.api.retry_attempts
        
    @abstractmethod
    async def fetch_data(self, symbol: str) -> IngestionResult:
        """
        Fetch data for a specific symbol.
        
        Args:
            symbol: Stock symbol to fetch data for
            
        Returns:
            IngestionResult containing the fetched data or error information
        """
        pass
    
    @abstractmethod
    async def validate_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate the fetched data.
        
        Args:
            data: Raw data to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        pass
    
    @abstractmethod
    async def transform_data(self, data: Dict[str, Any], symbol: str) -> TickData:
        """
        Transform raw data into standardized format.
        
        Args:
            data: Raw data from the source
            symbol: Stock symbol
            
        Returns:
            Standardized TickData object
        """
        pass
    
    async def ingest_single_symbol(self, symbol: str) -> IngestionResult:
        """
        Ingest data for a single symbol with retry logic.
        
        Args:
            symbol: Stock symbol to ingest
            
        Returns:
            IngestionResult with success status and data
        """
        for attempt in range(self._max_retries + 1):
            try:
                self.logger.info("Fetching data", symbol=symbol, attempt=attempt + 1)
                
                # Fetch data from source
                result = await self.fetch_data(symbol)
                
                if not result.success:
                    raise Exception(result.error)
                
                # Validate data
                if not await self.validate_data(result.data):
                    raise ValidationError("Data validation failed", model=TickData)
                
                # Transform data
                tick_data = await self.transform_data(result.data, symbol)
                
                # Record metrics
                self.metrics.increment_counter("data_ingested", labels={"source": self.source.value, "symbol": symbol})
                self.metrics.record_gauge("last_ingestion_timestamp", datetime.utcnow().timestamp(), 
                                        labels={"source": self.source.value, "symbol": symbol})
                
                self.logger.info("Data ingested successfully", symbol=symbol, price=tick_data.price)
                
                return IngestionResult(
                    success=True,
                    data=tick_data.dict(),
                    timestamp=datetime.utcnow(),
                    source=self.source
                )
                
            except Exception as e:
                self._retry_count += 1
                self.metrics.increment_counter("ingestion_errors", 
                                             labels={"source": self.source.value, "symbol": symbol, "error_type": type(e).__name__})
                
                self.logger.error("Data ingestion failed", 
                                symbol=symbol, 
                                attempt=attempt + 1, 
                                error=str(e),
                                retry_count=self._retry_count)
                
                if attempt < self._max_retries:
                    await asyncio.sleep(settings.api.retry_delay * (attempt + 1))
                else:
                    return IngestionResult(
                        success=False,
                        error=f"Failed after {self._max_retries + 1} attempts: {str(e)}",
                        timestamp=datetime.utcnow(),
                        source=self.source
                    )
    
    async def ingest_all_symbols(self) -> AsyncGenerator[IngestionResult, None]:
        """
        Ingest data for all configured symbols.
        
        Yields:
            IngestionResult for each symbol
        """
        self.logger.info("Starting ingestion for all symbols", symbols=self.symbols)
        
        for symbol in self.symbols:
            result = await self.ingest_single_symbol(symbol)
            yield result
            
            # Rate limiting
            await asyncio.sleep(60 / settings.api.requests_per_minute)
    
    async def start_continuous_ingestion(self, interval: int = None) -> None:
        """
        Start continuous data ingestion.
        
        Args:
            interval: Interval between ingestion cycles in seconds
        """
        interval = interval or settings.update_interval
        self.is_running = True
        
        self.logger.info("Starting continuous ingestion", 
                        interval=interval, 
                        symbols=self.symbols,
                        source=self.source.value)
        
        while self.is_running:
            try:
                async for result in self.ingest_all_symbols():
                    if not result.success:
                        self.logger.warning("Ingestion failed", 
                                          symbol=result.data.get("symbol") if result.data else None,
                                          error=result.error)
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                self.logger.error("Continuous ingestion error", error=str(e))
                await asyncio.sleep(interval)
    
    def stop_continuous_ingestion(self) -> None:
        """Stop continuous data ingestion."""
        self.is_running = False
        self.logger.info("Stopping continuous ingestion")
    
    async def health_check(self) -> bool:
        """
        Perform a health check on the ingestion service.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            # Try to fetch data for the first symbol
            if self.symbols:
                result = await self.ingest_single_symbol(self.symbols[0])
                return result.success
            return False
        except Exception as e:
            self.logger.error("Health check failed", error=str(e))
            return False


class DataIngestionManager:
    """
    Manager for multiple data ingestion services.
    
    Coordinates multiple ingestion services and provides unified interface.
    """
    
    def __init__(self):
        """Initialize the ingestion manager."""
        self.logger = structlog.get_logger("DataIngestionManager")
        self.ingestion_services: Dict[DataSource, BaseDataIngestion] = {}
        self.is_running = False
    
    def register_service(self, service: BaseDataIngestion) -> None:
        """
        Register an ingestion service.
        
        Args:
            service: Data ingestion service to register
        """
        self.ingestion_services[service.source] = service
        self.logger.info("Registered ingestion service", source=service.source.value)
    
    def unregister_service(self, source: DataSource) -> None:
        """
        Unregister an ingestion service.
        
        Args:
            source: Data source to unregister
        """
        if source in self.ingestion_services:
            service = self.ingestion_services[source]
            service.stop_continuous_ingestion()
            del self.ingestion_services[source]
            self.logger.info("Unregistered ingestion service", source=source.value)
    
    async def start_all_services(self) -> None:
        """Start all registered ingestion services."""
        self.is_running = True
        self.logger.info("Starting all ingestion services", count=len(self.ingestion_services))
        
        tasks = []
        for service in self.ingestion_services.values():
            task = asyncio.create_task(service.start_continuous_ingestion())
            tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    def stop_all_services(self) -> None:
        """Stop all registered ingestion services."""
        self.is_running = False
        for service in self.ingestion_services.values():
            service.stop_continuous_ingestion()
        self.logger.info("Stopped all ingestion services")
    
    async def health_check_all(self) -> Dict[DataSource, bool]:
        """
        Perform health check on all services.
        
        Returns:
            Dictionary mapping data sources to health status
        """
        health_status = {}
        for source, service in self.ingestion_services.items():
            health_status[source] = await service.health_check()
        return health_status
    
    def get_service(self, source: DataSource) -> Optional[BaseDataIngestion]:
        """
        Get a specific ingestion service.
        
        Args:
            source: Data source to get service for
            
        Returns:
            Ingestion service or None if not found
        """
        return self.ingestion_services.get(source)
