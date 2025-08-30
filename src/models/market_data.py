"""
Market data models for financial instruments.

This module defines the data models for market data including
tick data, OHLCV data, and various financial metrics.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, validator


class DataSource(str, Enum):
    """Enumeration of data sources."""
    YAHOO_FINANCE = "yahoo_finance"
    POLYGON = "polygon"
    ALPHA_VANTAGE = "alpha_vantage"
    MANUAL = "manual"


class MarketStatus(str, Enum):
    """Market status enumeration."""
    OPEN = "open"
    CLOSED = "closed"
    PRE_MARKET = "pre_market"
    AFTER_HOURS = "after_hours"
    HOLIDAY = "holiday"


class TickData(BaseModel):
    """
    Real-time tick data model.
    
    Represents a single tick of market data with price, volume,
    and timestamp information.
    """
    
    symbol: str = Field(..., description="Stock symbol (e.g., AAPL)")
    timestamp: datetime = Field(..., description="Timestamp of the tick")
    price: Decimal = Field(..., description="Current price")
    volume: int = Field(..., description="Volume traded")
    bid: Optional[Decimal] = Field(None, description="Best bid price")
    ask: Optional[Decimal] = Field(None, description="Best ask price")
    bid_size: Optional[int] = Field(None, description="Bid size")
    ask_size: Optional[int] = Field(None, description="Ask size")
    source: DataSource = Field(..., description="Data source")
    exchange: Optional[str] = Field(None, description="Exchange name")
    
    @validator("symbol")
    def validate_symbol(cls, v: str) -> str:
        """Validate and normalize symbol."""
        return v.upper().strip()
    
    @validator("price", "bid", "ask")
    def validate_prices(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        """Validate price values."""
        if v is not None and v <= 0:
            raise ValueError("Price must be positive")
        return v
    
    @validator("volume", "bid_size", "ask_size")
    def validate_sizes(cls, v: Optional[int]) -> Optional[int]:
        """Validate size values."""
        if v is not None and v < 0:
            raise ValueError("Size must be non-negative")
        return v
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class OHLCVData(BaseModel):
    """
    OHLCV (Open, High, Low, Close, Volume) data model.
    
    Represents aggregated market data for a specific time period.
    """
    
    symbol: str = Field(..., description="Stock symbol")
    timestamp: datetime = Field(..., description="Timestamp of the period")
    open_price: Decimal = Field(..., description="Opening price")
    high_price: Decimal = Field(..., description="Highest price")
    low_price: Decimal = Field(..., description="Lowest price")
    close_price: Decimal = Field(..., description="Closing price")
    volume: int = Field(..., description="Total volume")
    period: str = Field(..., description="Time period (1m, 5m, 1h, 1d, etc.)")
    source: DataSource = Field(..., description="Data source")
    
    @validator("symbol")
    def validate_symbol(cls, v: str) -> str:
        """Validate and normalize symbol."""
        return v.upper().strip()
    
    @validator("open_price", "high_price", "low_price", "close_price")
    def validate_prices(cls, v: Decimal) -> Decimal:
        """Validate price values."""
        if v <= 0:
            raise ValueError("Price must be positive")
        return v
    
    @validator("volume")
    def validate_volume(cls, v: int) -> int:
        """Validate volume."""
        if v < 0:
            raise ValueError("Volume must be non-negative")
        return v
    
    @property
    def price_change(self) -> Decimal:
        """Calculate price change from open to close."""
        return self.close_price - self.open_price
    
    @property
    def price_change_percent(self) -> Decimal:
        """Calculate price change percentage."""
        if self.open_price == 0:
            return Decimal("0")
        return (self.price_change / self.open_price) * 100
    
    @property
    def range(self) -> Decimal:
        """Calculate price range (high - low)."""
        return self.high_price - self.low_price
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class MarketMetrics(BaseModel):
    """
    Market metrics and indicators model.
    
    Contains calculated metrics and indicators for market analysis.
    """
    
    symbol: str = Field(..., description="Stock symbol")
    timestamp: datetime = Field(..., description="Timestamp of the metrics")
    
    # Price metrics
    current_price: Decimal = Field(..., description="Current price")
    price_change: Decimal = Field(..., description="Price change from previous period")
    price_change_percent: Decimal = Field(..., description="Price change percentage")
    
    # Volume metrics
    volume: int = Field(..., description="Current volume")
    avg_volume: Optional[int] = Field(None, description="Average volume")
    volume_ratio: Optional[Decimal] = Field(None, description="Volume ratio to average")
    
    # Technical indicators
    sma_20: Optional[Decimal] = Field(None, description="20-period Simple Moving Average")
    sma_50: Optional[Decimal] = Field(None, description="50-period Simple Moving Average")
    sma_200: Optional[Decimal] = Field(None, description="200-period Simple Moving Average")
    rsi: Optional[Decimal] = Field(None, description="Relative Strength Index")
    macd: Optional[Decimal] = Field(None, description="MACD line")
    macd_signal: Optional[Decimal] = Field(None, description="MACD signal line")
    
    # Volatility metrics
    volatility: Optional[Decimal] = Field(None, description="Price volatility")
    beta: Optional[Decimal] = Field(None, description="Beta coefficient")
    
    # Market data
    market_cap: Optional[int] = Field(None, description="Market capitalization")
    pe_ratio: Optional[Decimal] = Field(None, description="Price-to-Earnings ratio")
    dividend_yield: Optional[Decimal] = Field(None, description="Dividend yield")
    
    source: DataSource = Field(..., description="Data source")
    
    @validator("symbol")
    def validate_symbol(cls, v: str) -> str:
        """Validate and normalize symbol."""
        return v.upper().strip()
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class Alert(BaseModel):
    """
    Market alert model.
    
    Represents alerts triggered by market conditions or price movements.
    """
    
    id: str = Field(..., description="Unique alert identifier")
    symbol: str = Field(..., description="Stock symbol")
    alert_type: str = Field(..., description="Type of alert")
    condition: str = Field(..., description="Alert condition description")
    threshold: Optional[Decimal] = Field(None, description="Alert threshold value")
    current_value: Optional[Decimal] = Field(None, description="Current value that triggered alert")
    timestamp: datetime = Field(..., description="Alert timestamp")
    severity: str = Field(default="medium", description="Alert severity (low, medium, high, critical)")
    message: str = Field(..., description="Alert message")
    is_active: bool = Field(default=True, description="Whether alert is active")
    
    @validator("symbol")
    def validate_symbol(cls, v: str) -> str:
        """Validate and normalize symbol."""
        return v.upper().strip()
    
    @validator("severity")
    def validate_severity(cls, v: str) -> str:
        """Validate severity level."""
        valid_severities = ["low", "medium", "high", "critical"]
        if v.lower() not in valid_severities:
            raise ValueError(f"Severity must be one of {valid_severities}")
        return v.lower()
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class PortfolioPosition(BaseModel):
    """
    Portfolio position model.
    
    Represents a position in a portfolio with current value and P&L.
    """
    
    symbol: str = Field(..., description="Stock symbol")
    quantity: int = Field(..., description="Number of shares")
    average_price: Decimal = Field(..., description="Average purchase price")
    current_price: Decimal = Field(..., description="Current market price")
    market_value: Decimal = Field(..., description="Current market value")
    unrealized_pnl: Decimal = Field(..., description="Unrealized profit/loss")
    unrealized_pnl_percent: Decimal = Field(..., description="Unrealized P&L percentage")
    last_updated: datetime = Field(..., description="Last update timestamp")
    
    @validator("symbol")
    def validate_symbol(cls, v: str) -> str:
        """Validate and normalize symbol."""
        return v.upper().strip()
    
    @validator("quantity")
    def validate_quantity(cls, v: int) -> int:
        """Validate quantity."""
        if v == 0:
            raise ValueError("Quantity cannot be zero")
        return v
    
    @property
    def total_cost(self) -> Decimal:
        """Calculate total cost of position."""
        return self.quantity * self.average_price
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class MarketDataEvent(BaseModel):
    """
    Market data event wrapper.
    
    Wraps market data with metadata for event streaming.
    """
    
    event_id: str = Field(..., description="Unique event identifier")
    event_type: str = Field(..., description="Type of market data event")
    timestamp: datetime = Field(..., description="Event timestamp")
    data: Dict[str, Any] = Field(..., description="Event data payload")
    source: DataSource = Field(..., description="Data source")
    version: str = Field(default="1.0", description="Event schema version")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
