"""
Kafka producer for streaming market data.

This module provides a Kafka producer service for streaming
real-time market data with proper error handling and monitoring.
"""

import json
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import asdict

import structlog
from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import ValidationError

from src.config.settings import settings
from src.models.market_data import TickData, OHLCVData, MarketDataEvent, DataSource
from src.utils.monitoring import MetricsCollector


class KafkaProducerService:
    """
    Kafka producer service for streaming market data.
    
    Handles publishing market data events to Kafka topics with
    proper error handling, monitoring, and message delivery guarantees.
    """
    
    def __init__(self, topic_prefix: str = "finance"):
        """
        Initialize Kafka producer service.
        
        Args:
            topic_prefix: Prefix for Kafka topics
        """
        self.logger = structlog.get_logger("KafkaProducerService")
        self.metrics = MetricsCollector()
        self.topic_prefix = topic_prefix
        
        # Kafka configuration
        kafka_config = {
            'bootstrap.servers': settings.kafka.bootstrap_servers,
            'client.id': settings.kafka.producer_client_id,
            'acks': 'all',  # Wait for all replicas to acknowledge
            'retries': 3,
            'retry.backoff.ms': 1000,
            'batch.size': 16384,  # 16KB batch size
            'linger.ms': 10,  # Wait up to 10ms for batching
            'compression.type': 'snappy',
            'max.in.flight.requests.per.connection': 5,
            'enable.idempotence': True,  # Prevent duplicate messages
            'delivery.timeout.ms': 120000,  # 2 minutes
            'request.timeout.ms': 30000,  # 30 seconds
        }
        
        self.producer = Producer(kafka_config)
        self.admin_client = AdminClient({'bootstrap.servers': settings.kafka.bootstrap_servers})
        
        # Topic names
        self.market_data_topic = f"{topic_prefix}.market-data"
        self.alerts_topic = f"{topic_prefix}.alerts"
        self.ohlcv_topic = f"{topic_prefix}.ohlcv"
        self.metrics_topic = f"{topic_prefix}.metrics"
        
        # Ensure topics exist
        self._ensure_topics_exist()
    
    def _ensure_topics_exist(self) -> None:
        """Ensure required Kafka topics exist."""
        try:
            topics = [
                NewTopic(
                    self.market_data_topic,
                    num_partitions=3,
                    replication_factor=1
                ),
                NewTopic(
                    self.alerts_topic,
                    num_partitions=2,
                    replication_factor=1
                ),
                NewTopic(
                    self.ohlcv_topic,
                    num_partitions=2,
                    replication_factor=1
                ),
                NewTopic(
                    self.metrics_topic,
                    num_partitions=2,
                    replication_factor=1
                )
            ]
            
            # Create topics
            futures = self.admin_client.create_topics(topics)
            
            for topic, future in futures.items():
                try:
                    future.result()  # Wait for topic creation
                    self.logger.info("Topic created successfully", topic=topic)
                except KafkaException as e:
                    if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                        self.logger.info("Topic already exists", topic=topic)
                    else:
                        self.logger.error("Failed to create topic", topic=topic, error=str(e))
                        
        except Exception as e:
            self.logger.error("Failed to ensure topics exist", error=str(e))
    
    def _delivery_report(self, err: Optional[KafkaError], msg) -> None:
        """
        Delivery report callback for Kafka producer.
        
        Args:
            err: Kafka error if any
            msg: Kafka message
        """
        if err is not None:
            self.logger.error("Message delivery failed", 
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            error=str(err))
            self.metrics.increment_counter("kafka_delivery_errors", 
                                         labels={"topic": msg.topic(), "error": str(err)})
        else:
            self.logger.debug("Message delivered successfully", 
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset())
            self.metrics.increment_counter("kafka_messages_sent", 
                                         labels={"topic": msg.topic()})
    
    def _serialize_message(self, data: Any) -> bytes:
        """
        Serialize message data to JSON bytes.
        
        Args:
            data: Data to serialize
            
        Returns:
            Serialized bytes
        """
        try:
            if hasattr(data, 'dict'):
                # Pydantic model
                return json.dumps(data.dict(), default=str).encode('utf-8')
            elif hasattr(data, '__dict__'):
                # Dataclass or object
                return json.dumps(asdict(data) if hasattr(data, '__dataclass_fields__') else data.__dict__, 
                                default=str).encode('utf-8')
            else:
                # Regular dict or other types
                return json.dumps(data, default=str).encode('utf-8')
        except Exception as e:
            self.logger.error("Failed to serialize message", error=str(e))
            raise
    
    async def publish_tick_data(self, tick_data: TickData) -> bool:
        """
        Publish tick data to Kafka.
        
        Args:
            tick_data: Tick data to publish
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create market data event
            event = MarketDataEvent(
                event_id=str(uuid.uuid4()),
                event_type="tick_data",
                timestamp=datetime.utcnow(),
                data=tick_data.dict(),
                source=tick_data.source,
                version="1.0"
            )
            
            # Serialize message
            message = self._serialize_message(event)
            
            # Publish to Kafka
            self.producer.produce(
                topic=self.market_data_topic,
                key=tick_data.symbol.encode('utf-8'),
                value=message,
                callback=self._delivery_report
            )
            
            # Flush to ensure delivery
            self.producer.poll(0)
            
            self.logger.info("Tick data published", 
                           symbol=tick_data.symbol,
                           price=tick_data.price,
                           topic=self.market_data_topic)
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to publish tick data", 
                            symbol=tick_data.symbol,
                            error=str(e))
            self.metrics.increment_counter("publish_errors", 
                                         labels={"data_type": "tick_data", "error": type(e).__name__})
            return False
    
    async def publish_ohlcv_data(self, ohlcv_data: OHLCVData) -> bool:
        """
        Publish OHLCV data to Kafka.
        
        Args:
            ohlcv_data: OHLCV data to publish
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create market data event
            event = MarketDataEvent(
                event_id=str(uuid.uuid4()),
                event_type="ohlcv_data",
                timestamp=datetime.utcnow(),
                data=ohlcv_data.dict(),
                source=ohlcv_data.source,
                version="1.0"
            )
            
            # Serialize message
            message = self._serialize_message(event)
            
            # Publish to Kafka
            self.producer.produce(
                topic=self.ohlcv_topic,
                key=ohlcv_data.symbol.encode('utf-8'),
                value=message,
                callback=self._delivery_report
            )
            
            # Flush to ensure delivery
            self.producer.poll(0)
            
            self.logger.info("OHLCV data published", 
                           symbol=ohlcv_data.symbol,
                           period=ohlcv_data.period,
                           topic=self.ohlcv_topic)
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to publish OHLCV data", 
                            symbol=ohlcv_data.symbol,
                            error=str(e))
            self.metrics.increment_counter("publish_errors", 
                                         labels={"data_type": "ohlcv_data", "error": type(e).__name__})
            return False
    
    async def publish_alert(self, alert: Dict[str, Any]) -> bool:
        """
        Publish alert to Kafka.
        
        Args:
            alert: Alert data to publish
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Serialize message
            message = self._serialize_message(alert)
            
            # Publish to Kafka
            self.producer.produce(
                topic=self.alerts_topic,
                key=alert.get("symbol", "system").encode('utf-8'),
                value=message,
                callback=self._delivery_report
            )
            
            # Flush to ensure delivery
            self.producer.poll(0)
            
            self.logger.info("Alert published", 
                           alert_type=alert.get("alert_type"),
                           symbol=alert.get("symbol"),
                           topic=self.alerts_topic)
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to publish alert", 
                            alert_type=alert.get("alert_type"),
                            error=str(e))
            self.metrics.increment_counter("publish_errors", 
                                         labels={"data_type": "alert", "error": type(e).__name__})
            return False
    
    async def publish_metrics(self, metrics: Dict[str, Any]) -> bool:
        """
        Publish metrics to Kafka.
        
        Args:
            metrics: Metrics data to publish
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Add timestamp if not present
            if "timestamp" not in metrics:
                metrics["timestamp"] = datetime.utcnow().isoformat()
            
            # Serialize message
            message = self._serialize_message(metrics)
            
            # Publish to Kafka
            self.producer.produce(
                topic=self.metrics_topic,
                key=metrics.get("symbol", "system").encode('utf-8'),
                value=message,
                callback=self._delivery_report
            )
            
            # Flush to ensure delivery
            self.producer.poll(0)
            
            self.logger.debug("Metrics published", 
                            symbol=metrics.get("symbol"),
                            topic=self.metrics_topic)
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to publish metrics", error=str(e))
            self.metrics.increment_counter("publish_errors", 
                                         labels={"data_type": "metrics", "error": type(e).__name__})
            return False
    
    async def publish_batch(self, messages: List[Dict[str, Any]]) -> Dict[str, bool]:
        """
        Publish a batch of messages to Kafka.
        
        Args:
            messages: List of message dictionaries with 'topic', 'key', 'value' keys
            
        Returns:
            Dictionary mapping message IDs to success status
        """
        results = {}
        
        for i, message_data in enumerate(messages):
            try:
                topic = message_data.get("topic", self.market_data_topic)
                key = message_data.get("key", str(i)).encode('utf-8')
                value = self._serialize_message(message_data.get("value", {}))
                
                self.producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    callback=self._delivery_report
                )
                
                results[f"message_{i}"] = True
                
            except Exception as e:
                self.logger.error("Failed to publish batch message", 
                                message_index=i,
                                error=str(e))
                results[f"message_{i}"] = False
        
        # Flush all messages
        self.producer.flush()
        
        return results
    
    def flush(self, timeout: float = 30.0) -> int:
        """
        Flush all pending messages.
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            Number of messages remaining in queue
        """
        return self.producer.flush(timeout)
    
    def close(self) -> None:
        """Close the Kafka producer."""
        try:
            self.flush()
            self.producer.close()
            self.admin_client.close()
            self.logger.info("Kafka producer closed")
        except Exception as e:
            self.logger.error("Error closing Kafka producer", error=str(e))
    
    async def health_check(self) -> bool:
        """
        Perform health check on Kafka producer.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            # Try to get metadata for a topic
            metadata = self.producer.list_topics(timeout=10.0)
            return self.market_data_topic in metadata.topics
        except Exception as e:
            self.logger.error("Kafka producer health check failed", error=str(e))
            return False
