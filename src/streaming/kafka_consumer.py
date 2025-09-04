"""
Kafka consumer for processing market data streams.

This module provides a Kafka consumer service for processing
real-time market data events from Kafka topics.
"""

import json
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from dataclasses import asdict

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError

from src.config.settings import settings
from src.models.market_data import TickData, OHLCVData, MarketDataEvent, DataSource
from src.utils.monitoring import MetricsCollector


class KafkaConsumerService:
    """
    Kafka consumer service for processing market data streams.
    
    Handles consuming and processing market data events from Kafka topics
    with proper error handling and monitoring.
    """
    
    def __init__(self, topic_prefix: str = "finance"):
        """
        Initialize Kafka consumer service.
        
        Args:
            topic_prefix: Prefix for Kafka topics
        """
        self.logger = structlog.get_logger("KafkaConsumerService")
        self.metrics = MetricsCollector()
        self.topic_prefix = topic_prefix
        self.is_running = False
        
        # Kafka configuration
        kafka_config = {
            'bootstrap.servers': settings.kafka.bootstrap_servers,
            'group.id': settings.kafka.consumer_group_id,
            'auto.offset.reset': settings.kafka.auto_offset_reset,
            'enable.auto.commit': settings.kafka.enable_auto_commit,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 3000,
        }
        
        self.consumer = Consumer(kafka_config)
        
        # Topic names
        self.market_data_topic = f"{topic_prefix}.market-data"
        self.alerts_topic = f"{topic_prefix}.alerts"
        self.ohlcv_topic = f"{topic_prefix}.ohlcv"
        self.metrics_topic = f"{topic_prefix}.metrics"
        
        # Event handlers
        self.event_handlers: Dict[str, Callable] = {}
        
        # Subscribe to topics
        self._subscribe_to_topics()
    
    def _subscribe_to_topics(self) -> None:
        """Subscribe to Kafka topics."""
        topics = [
            self.market_data_topic,
            self.alerts_topic,
            self.ohlcv_topic,
            self.metrics_topic
        ]
        
        self.consumer.subscribe(topics)
        self.logger.info("Subscribed to Kafka topics", topics=topics)
    
    def register_event_handler(self, event_type: str, handler: Callable) -> None:
        """
        Register an event handler for a specific event type.
        
        Args:
            event_type: Type of event to handle
            handler: Function to handle the event
        """
        self.event_handlers[event_type] = handler
        self.logger.info("Registered event handler", event_type=event_type)
    
    def _deserialize_message(self, message: bytes) -> Optional[Dict[str, Any]]:
        """
        Deserialize Kafka message.
        
        Args:
            message: Raw message bytes
            
        Returns:
            Deserialized message data or None if failed
        """
        try:
            return json.loads(message.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self.logger.error("Failed to deserialize message", error=str(e))
            return None
    
    def _process_market_data_event(self, event_data: Dict[str, Any]) -> None:
        """
        Process market data event.
        
        Args:
            event_data: Event data dictionary
        """
        try:
            # Parse event
            event = MarketDataEvent(**event_data)
            
            # Handle based on event type
            if event.event_type == "tick_data":
                tick_data = TickData(**event.data)
                self._handle_tick_data(tick_data)
            elif event.event_type == "ohlcv_data":
                ohlcv_data = OHLCVData(**event.data)
                self._handle_ohlcv_data(ohlcv_data)
            else:
                self.logger.warning("Unknown event type", event_type=event.event_type)
            
            # Call registered handler if exists
            if event.event_type in self.event_handlers:
                self.event_handlers[event.event_type](event)
            
        except ValidationError as e:
            self.logger.error("Event validation failed", error=str(e))
            self.metrics.increment_counter("event_validation_errors", 
                                         labels={"event_type": event_data.get("event_type", "unknown")})
        except Exception as e:
            self.logger.error("Failed to process market data event", error=str(e))
            self.metrics.increment_counter("event_processing_errors", 
                                         labels={"event_type": event_data.get("event_type", "unknown")})
    
    def _handle_tick_data(self, tick_data: TickData) -> None:
        """
        Handle tick data event.
        
        Args:
            tick_data: Tick data to process
        """
        self.logger.info("Processing tick data", 
                        symbol=tick_data.symbol,
                        price=tick_data.price,
                        volume=tick_data.volume)
        
        # Record metrics
        self.metrics.increment_counter("tick_data_processed", 
                                     labels={"symbol": tick_data.symbol, "source": tick_data.source.value})
        
        # Real-time processing logic
        try:
            # Calculate basic technical indicators
            self._calculate_tick_indicators(tick_data)
            
            # Update portfolio positions (if applicable)
            self._update_portfolio_positions(tick_data)
            
            # Generate alerts for significant price movements
            self._check_price_alerts(tick_data)
            
            # Store in database
            self._store_tick_data(tick_data)
            
        except Exception as e:
            self.logger.error("Failed to process tick data", 
                            symbol=tick_data.symbol, 
                            error=str(e))
            self.metrics.increment_counter("tick_processing_errors", 
                                         labels={"symbol": tick_data.symbol})
    
    def _handle_ohlcv_data(self, ohlcv_data: OHLCVData) -> None:
        """
        Handle OHLCV data event.
        
        Args:
            ohlcv_data: OHLCV data to process
        """
        self.logger.info("Processing OHLCV data", 
                        symbol=ohlcv_data.symbol,
                        period=ohlcv_data.period,
                        close_price=ohlcv_data.close_price)
        
        # Record metrics
        self.metrics.increment_counter("ohlcv_data_processed", 
                                     labels={"symbol": ohlcv_data.symbol, "period": ohlcv_data.period})
        
        # OHLCV processing logic
        try:
            # Calculate moving averages
            self._calculate_moving_averages(ohlcv_data)
            
            # Detect chart patterns
            self._detect_chart_patterns(ohlcv_data)
            
            # Update analytics and metrics
            self._update_analytics(ohlcv_data)
            
            # Store OHLCV data
            self._store_ohlcv_data(ohlcv_data)
            
        except Exception as e:
            self.logger.error("Failed to process OHLCV data", 
                            symbol=ohlcv_data.symbol, 
                            error=str(e))
            self.metrics.increment_counter("ohlcv_processing_errors", 
                                         labels={"symbol": ohlcv_data.symbol})
    
    def _process_alert_event(self, alert_data: Dict[str, Any]) -> None:
        """
        Process alert event.
        
        Args:
            alert_data: Alert data dictionary
        """
        self.logger.info("Processing alert", 
                        alert_type=alert_data.get("alert_type"),
                        symbol=alert_data.get("symbol"),
                        severity=alert_data.get("severity"))
        
        # Record metrics
        self.metrics.increment_counter("alerts_processed", 
                                     labels={"alert_type": alert_data.get("alert_type", "unknown")})
        
        # Alert processing logic
        try:
            # Send notifications based on alert type
            self._send_alert_notifications(alert_data)
            
            # Update alert status in database
            self._update_alert_status(alert_data)
            
            # Trigger automated actions
            self._trigger_alert_actions(alert_data)
            
        except Exception as e:
            self.logger.error("Failed to process alert", 
                            alert_type=alert_data.get("alert_type"), 
                            error=str(e))
            self.metrics.increment_counter("alert_processing_errors", 
                                         labels={"alert_type": alert_data.get("alert_type", "unknown")})
    
    def _process_metrics_event(self, metrics_data: Dict[str, Any]) -> None:
        """
        Process metrics event.
        
        Args:
            metrics_data: Metrics data dictionary
        """
        self.logger.debug("Processing metrics", metrics_type=metrics_data.get("type"))
        
        # Record metrics
        self.metrics.increment_counter("metrics_processed", 
                                     labels={"metrics_type": metrics_data.get("type", "unknown")})
        
        # Metrics processing logic
        try:
            # Update real-time dashboards
            self._update_dashboard_metrics(metrics_data)
            
            # Trigger alerts for metric thresholds
            self._check_metric_thresholds(metrics_data)
            
            # Store metrics for historical analysis
            self._store_metrics_data(metrics_data)
            
        except Exception as e:
            self.logger.error("Failed to process metrics", 
                            metrics_type=metrics_data.get("type"), 
                            error=str(e))
            self.metrics.increment_counter("metrics_processing_errors", 
                                         labels={"metrics_type": metrics_data.get("type", "unknown")})
        # - Store for analysis
    
    async def start_consuming(self) -> None:
        """Start consuming messages from Kafka topics."""
        self.is_running = True
        self.logger.info("Starting Kafka consumer")
        
        while self.is_running:
            try:
                # Poll for messages
                message = self.consumer.poll(timeout=1.0)
                
                if message is None:
                    continue
                
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.debug("Reached end of partition", 
                                        topic=message.topic(),
                                        partition=message.partition())
                    else:
                        self.logger.error("Kafka error", 
                                        error=message.error(),
                                        topic=message.topic())
                    continue
                
                # Process message
                self._process_message(message)
                
                # Commit offset
                self.consumer.commit(message)
                
            except KeyboardInterrupt:
                self.logger.info("Received interrupt signal, stopping consumer")
                break
            except Exception as e:
                self.logger.error("Consumer error", error=str(e))
                await asyncio.sleep(1)
    
    def _process_message(self, message) -> None:
        """
        Process a single Kafka message.
        
        Args:
            message: Kafka message
        """
        try:
            # Deserialize message
            data = self._deserialize_message(message.value())
            if data is None:
                return
            
            # Process based on topic
            topic = message.topic()
            
            if topic == self.market_data_topic:
                self._process_market_data_event(data)
            elif topic == self.alerts_topic:
                self._process_alert_event(data)
            elif topic == self.metrics_topic:
                self._process_metrics_event(data)
            else:
                self.logger.warning("Unknown topic", topic=topic)
            
            # Record successful processing
            self.metrics.increment_counter("messages_processed", 
                                         labels={"topic": topic})
            
        except Exception as e:
            self.logger.error("Failed to process message", 
                            topic=message.topic(),
                            partition=message.partition(),
                            offset=message.offset(),
                            error=str(e))
            self.metrics.increment_counter("message_processing_errors", 
                                         labels={"topic": message.topic()})
    
    def stop_consuming(self) -> None:
        """Stop consuming messages."""
        self.is_running = False
        self.logger.info("Stopping Kafka consumer")
    
    def close(self) -> None:
        """Close the Kafka consumer."""
        try:
            self.stop_consuming()
            self.consumer.close()
            self.logger.info("Kafka consumer closed")
        except Exception as e:
            self.logger.error("Error closing Kafka consumer", error=str(e))
    
    async def health_check(self) -> bool:
        """
        Perform health check on Kafka consumer.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            # Check if consumer is subscribed to topics
            topics = self.consumer.assignment()
            return len(topics) > 0
        except Exception as e:
            self.logger.error("Kafka consumer health check failed", error=str(e))
            return False
    
    # Helper methods for processing logic
    
    def _calculate_tick_indicators(self, tick_data: TickData) -> None:
        """Calculate basic technical indicators for tick data."""
        # Placeholder for technical indicator calculations
        # In a real implementation, this would calculate RSI, MACD, etc.
        self.logger.debug("Calculating tick indicators", symbol=tick_data.symbol)
    
    def _update_portfolio_positions(self, tick_data: TickData) -> None:
        """Update portfolio positions based on tick data."""
        # Placeholder for portfolio position updates
        # In a real implementation, this would update P&L, positions, etc.
        self.logger.debug("Updating portfolio positions", symbol=tick_data.symbol)
    
    def _check_price_alerts(self, tick_data: TickData) -> None:
        """Check for price movement alerts."""
        # Placeholder for price alert logic
        # In a real implementation, this would check thresholds and generate alerts
        self.logger.debug("Checking price alerts", symbol=tick_data.symbol)
    
    def _store_tick_data(self, tick_data: TickData) -> None:
        """Store tick data in database."""
        # Placeholder for database storage
        # In a real implementation, this would insert into PostgreSQL
        self.logger.debug("Storing tick data", symbol=tick_data.symbol)
    
    def _calculate_moving_averages(self, ohlcv_data: OHLCVData) -> None:
        """Calculate moving averages for OHLCV data."""
        # Placeholder for moving average calculations
        self.logger.debug("Calculating moving averages", symbol=ohlcv_data.symbol)
    
    def _detect_chart_patterns(self, ohlcv_data: OHLCVData) -> None:
        """Detect chart patterns in OHLCV data."""
        # Placeholder for pattern detection
        self.logger.debug("Detecting chart patterns", symbol=ohlcv_data.symbol)
    
    def _update_analytics(self, ohlcv_data: OHLCVData) -> None:
        """Update analytics and metrics."""
        # Placeholder for analytics updates
        self.logger.debug("Updating analytics", symbol=ohlcv_data.symbol)
    
    def _store_ohlcv_data(self, ohlcv_data: OHLCVData) -> None:
        """Store OHLCV data in database."""
        # Placeholder for database storage
        self.logger.debug("Storing OHLCV data", symbol=ohlcv_data.symbol)
    
    def _send_alert_notifications(self, alert_data: Dict[str, Any]) -> None:
        """Send alert notifications."""
        # Placeholder for notification sending
        self.logger.debug("Sending alert notifications", alert_type=alert_data.get("alert_type"))
    
    def _update_alert_status(self, alert_data: Dict[str, Any]) -> None:
        """Update alert status in database."""
        # Placeholder for alert status updates
        self.logger.debug("Updating alert status", alert_type=alert_data.get("alert_type"))
    
    def _trigger_alert_actions(self, alert_data: Dict[str, Any]) -> None:
        """Trigger automated actions based on alerts."""
        # Placeholder for automated actions
        self.logger.debug("Triggering alert actions", alert_type=alert_data.get("alert_type"))
    
    def _update_dashboard_metrics(self, metrics_data: Dict[str, Any]) -> None:
        """Update real-time dashboard metrics."""
        # Placeholder for dashboard updates
        self.logger.debug("Updating dashboard metrics", metrics_type=metrics_data.get("type"))
    
    def _check_metric_thresholds(self, metrics_data: Dict[str, Any]) -> None:
        """Check metric thresholds and trigger alerts."""
        # Placeholder for threshold checking
        self.logger.debug("Checking metric thresholds", metrics_type=metrics_data.get("type"))
    
    def _store_metrics_data(self, metrics_data: Dict[str, Any]) -> None:
        """Store metrics data for historical analysis."""
        # Placeholder for metrics storage
        self.logger.debug("Storing metrics data", metrics_type=metrics_data.get("type"))
