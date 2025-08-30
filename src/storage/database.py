"""
Database service for TimescaleDB integration.

This module provides database operations for storing and retrieving
market data using TimescaleDB (PostgreSQL with time-series extensions).
"""

import asyncio
from datetime import datetime
from typing import List, Optional, Dict, Any
from decimal import Decimal

import structlog
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

from src.config.settings import settings
from src.models.market_data import TickData, OHLCVData, Alert, MarketMetrics
from src.utils.monitoring import MetricsCollector


class DatabaseService:
    """
    Database service for TimescaleDB operations.
    
    Handles all database operations including connection management,
    data insertion, queries, and time-series optimizations.
    """
    
    def __init__(self):
        """Initialize the database service."""
        self.logger = structlog.get_logger("DatabaseService")
        self.metrics = MetricsCollector()
        self.connection_string = settings.database.connection_string
        
        # SQLAlchemy engine
        self.engine = create_engine(
            self.connection_string,
            poolclass=QueuePool,
            pool_size=settings.database.pool_size,
            max_overflow=settings.database.max_overflow,
            echo=settings.database.echo
        )
        
        # Session factory
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Initialize database
        self._initialize_database()
    
    def _initialize_database(self) -> None:
        """Initialize database tables."""
        try:
            with self.engine.connect() as conn:
                # Create tables
                self._create_tables(conn)
                conn.commit()
                
            self.logger.info("Database initialized successfully")
            
        except Exception as e:
            self.logger.error("Failed to initialize database", error=str(e))
            raise
    
    def _create_tables(self, conn) -> None:
        """Create database tables."""
        # Tick data table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tick_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                price DECIMAL(10, 4) NOT NULL,
                volume INTEGER NOT NULL,
                bid DECIMAL(10, 4),
                ask DECIMAL(10, 4),
                bid_size INTEGER,
                ask_size INTEGER,
                source VARCHAR(50) NOT NULL,
                exchange VARCHAR(20),
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        
        # OHLCV data table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ohlcv_data (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                open_price DECIMAL(10, 4) NOT NULL,
                high_price DECIMAL(10, 4) NOT NULL,
                low_price DECIMAL(10, 4) NOT NULL,
                close_price DECIMAL(10, 4) NOT NULL,
                volume INTEGER NOT NULL,
                period VARCHAR(10) NOT NULL,
                source VARCHAR(50) NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        
        # Market metrics table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS market_metrics (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                current_price DECIMAL(10, 4) NOT NULL,
                price_change DECIMAL(10, 4) NOT NULL,
                price_change_percent DECIMAL(10, 4) NOT NULL,
                volume INTEGER NOT NULL,
                avg_volume INTEGER,
                volume_ratio DECIMAL(10, 4),
                sma_20 DECIMAL(10, 4),
                sma_50 DECIMAL(10, 4),
                sma_200 DECIMAL(10, 4),
                rsi DECIMAL(10, 4),
                macd DECIMAL(10, 4),
                macd_signal DECIMAL(10, 4),
                volatility DECIMAL(10, 4),
                beta DECIMAL(10, 4),
                market_cap BIGINT,
                pe_ratio DECIMAL(10, 4),
                dividend_yield DECIMAL(10, 4),
                source VARCHAR(50) NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        
        # Alerts table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS alerts (
                id VARCHAR(50) PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                alert_type VARCHAR(50) NOT NULL,
                condition TEXT NOT NULL,
                threshold DECIMAL(10, 4),
                current_value DECIMAL(10, 4),
                timestamp TIMESTAMPTZ NOT NULL,
                severity VARCHAR(20) NOT NULL,
                message TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        
        # Create indexes
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tick_data_symbol_timestamp ON tick_data (symbol, timestamp DESC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ohlcv_data_symbol_timestamp ON ohlcv_data (symbol, timestamp DESC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_market_metrics_symbol_timestamp ON market_metrics (symbol, timestamp DESC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_alerts_symbol_timestamp ON alerts (symbol, timestamp DESC)"))
    

    
    async def insert_tick_data(self, tick_data: TickData) -> bool:
        """
        Insert tick data into database.
        
        Args:
            tick_data: Tick data to insert
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.SessionLocal() as session:
                query = text("""
                    INSERT INTO tick_data (symbol, timestamp, price, volume, bid, ask, bid_size, ask_size, source, exchange)
                    VALUES (:symbol, :timestamp, :price, :volume, :bid, :ask, :bid_size, :ask_size, :source, :exchange)
                """)
                
                session.execute(query, {
                    'symbol': tick_data.symbol,
                    'timestamp': tick_data.timestamp,
                    'price': float(tick_data.price),
                    'volume': tick_data.volume,
                    'bid': float(tick_data.bid) if tick_data.bid else None,
                    'ask': float(tick_data.ask) if tick_data.ask else None,
                    'bid_size': tick_data.bid_size,
                    'ask_size': tick_data.ask_size,
                    'source': tick_data.source.value,
                    'exchange': tick_data.exchange
                })
                
                session.commit()
                
                self.metrics.increment_counter("tick_data_inserted", 
                                             labels={"symbol": tick_data.symbol})
                
                return True
                
        except Exception as e:
            self.logger.error("Failed to insert tick data", 
                            symbol=tick_data.symbol,
                            error=str(e))
            self.metrics.increment_counter("database_errors", 
                                         labels={"operation": "insert_tick_data"})
            return False
    
    async def insert_ohlcv_data(self, ohlcv_data: OHLCVData) -> bool:
        """
        Insert OHLCV data into database.
        
        Args:
            ohlcv_data: OHLCV data to insert
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.SessionLocal() as session:
                query = text("""
                    INSERT INTO ohlcv_data (symbol, timestamp, open_price, high_price, low_price, close_price, volume, period, source)
                    VALUES (:symbol, :timestamp, :open_price, :high_price, :low_price, :close_price, :volume, :period, :source)
                """)
                
                session.execute(query, {
                    'symbol': ohlcv_data.symbol,
                    'timestamp': ohlcv_data.timestamp,
                    'open_price': float(ohlcv_data.open_price),
                    'high_price': float(ohlcv_data.high_price),
                    'low_price': float(ohlcv_data.low_price),
                    'close_price': float(ohlcv_data.close_price),
                    'volume': ohlcv_data.volume,
                    'period': ohlcv_data.period,
                    'source': ohlcv_data.source.value
                })
                
                session.commit()
                
                self.metrics.increment_counter("ohlcv_data_inserted", 
                                             labels={"symbol": ohlcv_data.symbol, "period": ohlcv_data.period})
                
                return True
                
        except Exception as e:
            self.logger.error("Failed to insert OHLCV data", 
                            symbol=ohlcv_data.symbol,
                            error=str(e))
            self.metrics.increment_counter("database_errors", 
                                         labels={"operation": "insert_ohlcv_data"})
            return False
    
    async def insert_market_metrics(self, metrics: MarketMetrics) -> bool:
        """
        Insert market metrics into database.
        
        Args:
            metrics: Market metrics to insert
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.SessionLocal() as session:
                query = text("""
                    INSERT INTO market_metrics (
                        symbol, timestamp, current_price, price_change, price_change_percent,
                        volume, avg_volume, volume_ratio, sma_20, sma_50, sma_200,
                        rsi, macd, macd_signal, volatility, beta, market_cap, pe_ratio,
                        dividend_yield, source
                    ) VALUES (
                        :symbol, :timestamp, :current_price, :price_change, :price_change_percent,
                        :volume, :avg_volume, :volume_ratio, :sma_20, :sma_50, :sma_200,
                        :rsi, :macd, :macd_signal, :volatility, :beta, :market_cap, :pe_ratio,
                        :dividend_yield, :source
                    )
                """)
                
                session.execute(query, {
                    'symbol': metrics.symbol,
                    'timestamp': metrics.timestamp,
                    'current_price': float(metrics.current_price),
                    'price_change': float(metrics.price_change),
                    'price_change_percent': float(metrics.price_change_percent),
                    'volume': metrics.volume,
                    'avg_volume': metrics.avg_volume,
                    'volume_ratio': float(metrics.volume_ratio) if metrics.volume_ratio else None,
                    'sma_20': float(metrics.sma_20) if metrics.sma_20 else None,
                    'sma_50': float(metrics.sma_50) if metrics.sma_50 else None,
                    'sma_200': float(metrics.sma_200) if metrics.sma_200 else None,
                    'rsi': float(metrics.rsi) if metrics.rsi else None,
                    'macd': float(metrics.macd) if metrics.macd else None,
                    'macd_signal': float(metrics.macd_signal) if metrics.macd_signal else None,
                    'volatility': float(metrics.volatility) if metrics.volatility else None,
                    'beta': float(metrics.beta) if metrics.beta else None,
                    'market_cap': metrics.market_cap,
                    'pe_ratio': float(metrics.pe_ratio) if metrics.pe_ratio else None,
                    'dividend_yield': float(metrics.dividend_yield) if metrics.dividend_yield else None,
                    'source': metrics.source.value
                })
                
                session.commit()
                
                self.metrics.increment_counter("market_metrics_inserted", 
                                             labels={"symbol": metrics.symbol})
                
                return True
                
        except Exception as e:
            self.logger.error("Failed to insert market metrics", 
                            symbol=metrics.symbol,
                            error=str(e))
            self.metrics.increment_counter("database_errors", 
                                         labels={"operation": "insert_market_metrics"})
            return False
    
    async def insert_alert(self, alert: Alert) -> bool:
        """
        Insert alert into database.
        
        Args:
            alert: Alert to insert
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.SessionLocal() as session:
                query = text("""
                    INSERT INTO alerts (id, symbol, alert_type, condition, threshold, current_value, timestamp, severity, message, is_active)
                    VALUES (:id, :symbol, :alert_type, :condition, :threshold, :current_value, :timestamp, :severity, :message, :is_active)
                    ON CONFLICT (id) DO UPDATE SET
                        current_value = EXCLUDED.current_value,
                        timestamp = EXCLUDED.timestamp,
                        is_active = EXCLUDED.is_active
                """)
                
                session.execute(query, {
                    'id': alert.id,
                    'symbol': alert.symbol,
                    'alert_type': alert.alert_type,
                    'condition': alert.condition,
                    'threshold': float(alert.threshold) if alert.threshold else None,
                    'current_value': float(alert.current_value) if alert.current_value else None,
                    'timestamp': alert.timestamp,
                    'severity': alert.severity,
                    'message': alert.message,
                    'is_active': alert.is_active
                })
                
                session.commit()
                
                self.metrics.increment_counter("alerts_inserted", 
                                             labels={"alert_type": alert.alert_type, "severity": alert.severity})
                
                return True
                
        except Exception as e:
            self.logger.error("Failed to insert alert", 
                            alert_id=alert.id,
                            error=str(e))
            self.metrics.increment_counter("database_errors", 
                                         labels={"operation": "insert_alert"})
            return False
    
    async def get_latest_tick_data(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get latest tick data for a symbol.
        
        Args:
            symbol: Stock symbol
            limit: Number of records to return
            
        Returns:
            List of tick data records
        """
        try:
            with self.SessionLocal() as session:
                query = text("""
                    SELECT * FROM tick_data 
                    WHERE symbol = :symbol 
                    ORDER BY timestamp DESC 
                    LIMIT :limit
                """)
                
                result = session.execute(query, {'symbol': symbol, 'limit': limit})
                return [dict(row) for row in result]
                
        except Exception as e:
            self.logger.error("Failed to get latest tick data", 
                            symbol=symbol,
                            error=str(e))
            return []
    
    async def get_ohlcv_data(self, symbol: str, period: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """
        Get OHLCV data for a symbol within a time range.
        
        Args:
            symbol: Stock symbol
            period: Time period (1m, 5m, 1h, 1d, etc.)
            start_time: Start time
            end_time: End time
            
        Returns:
            List of OHLCV data records
        """
        try:
            with self.SessionLocal() as session:
                query = text("""
                    SELECT * FROM ohlcv_data 
                    WHERE symbol = :symbol 
                    AND period = :period
                    AND timestamp BETWEEN :start_time AND :end_time
                    ORDER BY timestamp ASC
                """)
                
                result = session.execute(query, {
                    'symbol': symbol,
                    'period': period,
                    'start_time': start_time,
                    'end_time': end_time
                })
                return [dict(row) for row in result]
                
        except Exception as e:
            self.logger.error("Failed to get OHLCV data", 
                            symbol=symbol,
                            period=period,
                            error=str(e))
            return []
    
    async def health_check(self) -> bool:
        """
        Perform health check on database.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                return True
        except Exception as e:
            self.logger.error("Database health check failed", error=str(e))
            return False
    
    def close(self) -> None:
        """Close database connections."""
        try:
            self.engine.dispose()
            self.logger.info("Database connections closed")
        except Exception as e:
            self.logger.error("Error closing database connections", error=str(e))
