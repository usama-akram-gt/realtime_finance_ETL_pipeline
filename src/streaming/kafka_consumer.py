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
        
        # TODO: Add real-time processing logic
        # - Calculate technical indicators
        # - Update portfolio positions
        # - Generate alerts
        # - Store in database
    
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
        
        # TODO: Add OHLCV processing logic
        # - Calculate moving averages
        # - Detect patterns
        # - Update analytics
    
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
        
        # TODO: Add alert processing logic
        # - Send notifications
        # - Update alert status
        # - Trigger actions
    
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
        
        # TODO: Add metrics processing logic
        # - Update dashboards
        # - Trigger alerts
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
