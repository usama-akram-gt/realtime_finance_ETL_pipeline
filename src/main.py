"""
Main application entry point for the Finance Data Platform.

This module serves as the main orchestrator for the finance data streaming
platform, coordinating data ingestion, processing, and streaming services.
"""

import asyncio
import signal
import sys
from typing import List
import structlog

from src.config.settings import settings
from src.ingestion.base import DataIngestionManager
from src.ingestion.yahoo_finance import YahooFinanceIngestion, YahooFinanceBatchIngestion
from src.streaming.kafka_producer import KafkaProducerService
from src.streaming.kafka_consumer import KafkaConsumerService
from src.storage.database import DatabaseService
from src.utils.monitoring import health_checker, performance_monitor, metrics_collector


class FinanceDataPlatform:
    """
    Main finance data platform orchestrator.
    
    Coordinates all components of the finance data streaming platform
    including data ingestion, processing, and streaming services.
    """
    
    def __init__(self):
        """Initialize the finance data platform."""
        self.logger = structlog.get_logger("FinanceDataPlatform")
        self.ingestion_manager = DataIngestionManager()
        self.kafka_producer = KafkaProducerService()
        self.kafka_consumer = KafkaConsumerService()
        self.database_service = DatabaseService()
        self.is_running = False
        self.tasks: List[asyncio.Task] = []
        
        # Setup logging
        self._setup_logging()
        
        # Register health checks
        self._register_health_checks()
    
    def _setup_logging(self) -> None:
        """Setup structured logging configuration."""
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
        
        self.logger.info("Finance Data Platform initialized", 
                        environment=settings.environment,
                        symbols=settings.symbols)
    
    def _register_health_checks(self) -> None:
        """Register health checks for system components."""
        # Register Kafka producer health check
        health_checker.register_health_check(
            "kafka_producer",
            self.kafka_producer.health_check
        )
        
        # Register ingestion services health checks
        async def ingestion_health_check():
            return await self.ingestion_manager.health_check_all()
        
        health_checker.register_health_check(
            "ingestion_services",
            ingestion_health_check
        )
        
        # Register Kafka consumer health check
        health_checker.register_health_check(
            "kafka_consumer",
            self.kafka_consumer.health_check
        )
        
        # Register database health check
        health_checker.register_health_check(
            "database",
            self.database_service.health_check
        )
    
    async def setup_ingestion_services(self) -> None:
        """Setup and register data ingestion services."""
        try:
            # Register Yahoo Finance ingestion service
            if settings.api.yahoo_finance_enabled:
                yahoo_service = YahooFinanceIngestion(settings.symbols)
                self.ingestion_manager.register_service(yahoo_service)
                
                # Also register batch service for efficiency
                batch_service = YahooFinanceBatchIngestion(settings.symbols, batch_size=5)
                self.ingestion_manager.register_service(batch_service)
                
                self.logger.info("Yahoo Finance ingestion services registered")
            
            # TODO: Add other ingestion services (Polygon, Alpha Vantage)
            # if settings.api.polygon_enabled:
            #     polygon_service = PolygonIngestion(settings.symbols)
            #     self.ingestion_manager.register_service(polygon_service)
            
            # if settings.api.alpha_vantage_enabled:
            #     alpha_service = AlphaVantageIngestion(settings.symbols)
            #     self.ingestion_manager.register_service(alpha_service)
            
        except Exception as e:
            self.logger.error("Failed to setup ingestion services", error=str(e))
            raise
    
    async def start_services(self) -> None:
        """Start all platform services."""
        try:
            self.is_running = True
            
            # Start ingestion services
            ingestion_task = asyncio.create_task(
                self.ingestion_manager.start_all_services()
            )
            self.tasks.append(ingestion_task)
            
            # Start Kafka consumer
            consumer_task = asyncio.create_task(self.kafka_consumer.start_consuming())
            self.tasks.append(consumer_task)
            
            # Start health monitoring
            health_task = asyncio.create_task(self._health_monitoring_loop())
            self.tasks.append(health_task)
            
            # Start metrics publishing
            metrics_task = asyncio.create_task(self._metrics_publishing_loop())
            self.tasks.append(metrics_task)
            
            self.logger.info("All services started successfully")
            
        except Exception as e:
            self.logger.error("Failed to start services", error=str(e))
            raise
    
    async def stop_services(self) -> None:
        """Stop all platform services."""
        try:
            self.is_running = False
            
            # Cancel all tasks
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to complete
            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)
            
            # Stop ingestion services
            self.ingestion_manager.stop_all_services()
            
            # Close Kafka producer and consumer
            self.kafka_producer.close()
            self.kafka_consumer.close()
            
            # Close database connections
            self.database_service.close()
            
            self.logger.info("All services stopped successfully")
            
        except Exception as e:
            self.logger.error("Error stopping services", error=str(e))
    
    async def _health_monitoring_loop(self) -> None:
        """Health monitoring loop."""
        while self.is_running:
            try:
                # Run health checks
                health_results = await health_checker.run_health_checks()
                
                # Log health status
                overall_health = health_checker.get_overall_health()
                self.logger.info("Health check results", 
                               overall=overall_health["overall"],
                               healthy_count=overall_health["healthy_count"],
                               unhealthy_count=overall_health["unhealthy_count"])
                
                # Publish health metrics
                await self.kafka_producer.publish_metrics({
                    "type": "health_check",
                    "overall_health": overall_health["overall"],
                    "healthy_count": overall_health["healthy_count"],
                    "unhealthy_count": overall_health["unhealthy_count"],
                    "total_count": overall_health["total_count"]
                })
                
                # Wait before next health check
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error("Health monitoring error", error=str(e))
                await asyncio.sleep(60)
    
    async def _metrics_publishing_loop(self) -> None:
        """Metrics publishing loop."""
        while self.is_running:
            try:
                # Get performance statistics
                perf_stats = performance_monitor.get_all_stats()
                
                # Publish performance metrics
                await self.kafka_producer.publish_metrics({
                    "type": "performance_stats",
                    "stats": perf_stats
                })
                
                # Wait before next metrics publish
                await asyncio.sleep(300)  # Publish every 5 minutes
                
            except Exception as e:
                self.logger.error("Metrics publishing error", error=str(e))
                await asyncio.sleep(300)
    
    async def run(self) -> None:
        """Run the finance data platform."""
        try:
            # Setup services
            await self.setup_ingestion_services()
            
            # Start services
            await self.start_services()
            
            # Keep running until interrupted
            while self.is_running:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            self.logger.error("Platform error", error=str(e))
            raise
        finally:
            await self.stop_services()


def setup_signal_handlers(platform: FinanceDataPlatform) -> None:
    """Setup signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        platform.logger.info(f"Received signal {signum}, initiating shutdown...")
        platform.is_running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def main():
    """Main application entry point."""
    platform = FinanceDataPlatform()
    
    # Setup signal handlers
    setup_signal_handlers(platform)
    
    try:
        await platform.run()
    except Exception as e:
        platform.logger.error("Application failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
