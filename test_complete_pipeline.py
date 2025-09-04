#!/usr/bin/env python3
"""
Comprehensive test pipeline for the Finance Data Platform.

This script tests all components of the finance data pipeline including:
- Data ingestion from Yahoo Finance
- Kafka message streaming
- Database storage
- Health checks
- Error handling
"""

import asyncio
import sys
import time
from datetime import datetime
from typing import List, Dict, Any
import structlog

# Add src to path
sys.path.insert(0, 'src')

from src.config.settings import settings
from src.ingestion.yahoo_finance import YahooFinanceIngestion
from src.streaming.kafka_producer import KafkaProducerService
from src.streaming.kafka_consumer import KafkaConsumerService
from src.storage.database import DatabaseService
from src.utils.monitoring import health_checker, metrics_collector
from src.models.market_data import TickData, DataSource


class PipelineTester:
    """Comprehensive pipeline tester."""
    
    def __init__(self):
        """Initialize the pipeline tester."""
        self.logger = structlog.get_logger("PipelineTester")
        self.test_results: Dict[str, bool] = {}
        self.start_time = time.time()
        
        # Initialize services
        self.ingestion_service = YahooFinanceIngestion(settings.symbols)
        self.kafka_producer = KafkaProducerService()
        self.kafka_consumer = KafkaConsumerService()
        self.database_service = DatabaseService()
    
    async def run_all_tests(self) -> bool:
        """Run all pipeline tests."""
        self.logger.info("Starting comprehensive pipeline tests")
        
        tests = [
            ("Database Connection", self.test_database_connection),
            ("Data Ingestion", self.test_data_ingestion),
            ("Kafka Producer", self.test_kafka_producer),
            ("Kafka Consumer", self.test_kafka_consumer),
            ("End-to-End Flow", self.test_end_to_end_flow),
            ("Health Checks", self.test_health_checks),
            ("Error Handling", self.test_error_handling)
        ]
        
        for test_name, test_func in tests:
            try:
                self.logger.info(f"Running test: {test_name}")
                result = await test_func()
                self.test_results[test_name] = result
                
                if result:
                    self.logger.info(f"✅ {test_name} - PASSED")
                else:
                    self.logger.error(f"❌ {test_name} - FAILED")
                    
            except Exception as e:
                self.logger.error(f"❌ {test_name} - ERROR: {str(e)}")
                self.test_results[test_name] = False
        
        return self._print_summary()
    
    async def test_database_connection(self) -> bool:
        """Test database connection and initialization."""
        try:
            # Test database connection
            await self.database_service.initialize()
            
            # Test table creation
            await self.database_service._initialize_database()
            
            # Test basic query
            result = await self.database_service.execute_query("SELECT 1 as test")
            return result is not None
            
        except Exception as e:
            self.logger.error("Database test failed", error=str(e))
            return False
    
    async def test_data_ingestion(self) -> bool:
        """Test data ingestion from Yahoo Finance."""
        try:
            # Test fetching data for a single symbol
            result = await self.ingestion_service.fetch_data("AAPL")
            
            if not result.success:
                self.logger.error("Data ingestion failed", error=result.error)
                return False
            
            # Validate the data structure
            if not isinstance(result.data, TickData):
                self.logger.error("Invalid data type returned")
                return False
            
            # Check required fields
            tick_data = result.data
            if not all([tick_data.symbol, tick_data.price, tick_data.timestamp]):
                self.logger.error("Missing required fields in tick data")
                return False
            
            self.logger.info("Data ingestion test passed", 
                           symbol=tick_data.symbol,
                           price=float(tick_data.price))
            return True
            
        except Exception as e:
            self.logger.error("Data ingestion test failed", error=str(e))
            return False
    
    async def test_kafka_producer(self) -> bool:
        """Test Kafka producer functionality."""
        try:
            # Initialize producer
            await self.kafka_producer.initialize()
            
            # Create test message
            test_tick = TickData(
                symbol="TEST",
                price=100.0,
                volume=1000,
                timestamp=datetime.utcnow(),
                source=DataSource.YAHOO_FINANCE
            )
            
            # Send test message
            success = await self.kafka_producer.send_tick_data(test_tick)
            
            if not success:
                self.logger.error("Failed to send test message to Kafka")
                return False
            
            self.logger.info("Kafka producer test passed")
            return True
            
        except Exception as e:
            self.logger.error("Kafka producer test failed", error=str(e))
            return False
    
    async def test_kafka_consumer(self) -> bool:
        """Test Kafka consumer functionality."""
        try:
            # Initialize consumer
            await self.kafka_consumer.initialize()
            
            # Test health check
            health = await self.kafka_consumer.health_check()
            
            if not health:
                self.logger.error("Kafka consumer health check failed")
                return False
            
            self.logger.info("Kafka consumer test passed")
            return True
            
        except Exception as e:
            self.logger.error("Kafka consumer test failed", error=str(e))
            return False
    
    async def test_end_to_end_flow(self) -> bool:
        """Test complete end-to-end data flow."""
        try:
            # 1. Fetch data from Yahoo Finance
            result = await self.ingestion_service.fetch_data("AAPL")
            if not result.success:
                return False
            
            tick_data = result.data
            
            # 2. Send to Kafka
            kafka_success = await self.kafka_producer.send_tick_data(tick_data)
            if not kafka_success:
                return False
            
            # 3. Store in database
            db_success = await self.database_service.store_tick_data(tick_data)
            if not db_success:
                return False
            
            self.logger.info("End-to-end flow test passed")
            return True
            
        except Exception as e:
            self.logger.error("End-to-end flow test failed", error=str(e))
            return False
    
    async def test_health_checks(self) -> bool:
        """Test health check functionality."""
        try:
            # Register health checks
            health_checker.register_health_check("database", self.database_service.health_check)
            health_checker.register_health_check("kafka_producer", self.kafka_producer.health_check)
            health_checker.register_health_check("kafka_consumer", self.kafka_consumer.health_check)
            
            # Run health checks
            results = await health_checker.run_health_checks()
            
            # Check if all health checks passed
            all_healthy = all(result["healthy"] for result in results.values())
            
            if not all_healthy:
                self.logger.error("Some health checks failed", results=results)
                return False
            
            self.logger.info("Health checks test passed")
            return True
            
        except Exception as e:
            self.logger.error("Health checks test failed", error=str(e))
            return False
    
    async def test_error_handling(self) -> bool:
        """Test error handling and recovery."""
        try:
            # Test with invalid symbol
            result = await self.ingestion_service.fetch_data("INVALID_SYMBOL_12345")
            
            # Should handle gracefully (either return error or empty result)
            if result.success and result.data is None:
                # This is acceptable - service handled invalid symbol gracefully
                pass
            elif not result.success:
                # This is also acceptable - service returned error
                pass
            else:
                # Unexpected - got data for invalid symbol
                self.logger.warning("Unexpected: got data for invalid symbol")
            
            # Test database with invalid query
            try:
                await self.database_service.execute_query("INVALID SQL QUERY")
                # Should not reach here
                return False
            except Exception:
                # Expected - database should reject invalid query
                pass
            
            self.logger.info("Error handling test passed")
            return True
            
        except Exception as e:
            self.logger.error("Error handling test failed", error=str(e))
            return False
    
    def _print_summary(self) -> bool:
        """Print test summary and return overall result."""
        duration = time.time() - self.start_time
        
        print("\n" + "="*60)
        print("PIPELINE TEST SUMMARY")
        print("="*60)
        
        passed = 0
        total = len(self.test_results)
        
        for test_name, result in self.test_results.items():
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"{test_name:<25} {status}")
            if result:
                passed += 1
        
        print("-"*60)
        print(f"Tests Passed: {passed}/{total}")
        print(f"Success Rate: {(passed/total)*100:.1f}%")
        print(f"Duration: {duration:.2f} seconds")
        print("="*60)
        
        return passed == total
    
    async def cleanup(self):
        """Cleanup resources."""
        try:
            await self.kafka_producer.close()
            await self.kafka_consumer.close()
            await self.database_service.close()
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error("Cleanup failed", error=str(e))


async def main():
    """Main test function."""
    # Setup logging
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
    
    logger = structlog.get_logger("Main")
    logger.info("Starting Finance Data Pipeline Tests")
    
    tester = PipelineTester()
    
    try:
        success = await tester.run_all_tests()
        
        if success:
            logger.info("All tests passed! Pipeline is ready for production.")
            sys.exit(0)
        else:
            logger.error("Some tests failed. Please check the logs.")
            sys.exit(1)
            
    except Exception as e:
        logger.error("Test execution failed", error=str(e))
        sys.exit(1)
        
    finally:
        await tester.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
