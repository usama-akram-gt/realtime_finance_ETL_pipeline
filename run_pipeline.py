#!/usr/bin/env python3
"""
Finance Data Pipeline Runner

This script provides a simple interface to run the complete finance data pipeline
with all components including Kafka, PostgreSQL, Spark, and Airflow.
"""

import asyncio
import sys
import time
import subprocess
import signal
from pathlib import Path
from typing import List, Dict, Any
import structlog

# Add src to path
sys.path.insert(0, 'src')

from src.config.settings import settings
from src.main import FinanceDataPlatform


class PipelineRunner:
    """Main pipeline runner with service management."""
    
    def __init__(self):
        """Initialize the pipeline runner."""
        self.logger = structlog.get_logger("PipelineRunner")
        self.platform = None
        self.docker_process = None
        self.is_running = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.is_running = False
    
    async def start_infrastructure(self) -> bool:
        """Start Docker infrastructure services."""
        try:
            self.logger.info("Starting Docker infrastructure services...")
            
            # Start Docker Compose services
            cmd = ["docker-compose", "up", "-d"]
            self.docker_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait for services to start
            stdout, stderr = self.docker_process.communicate()
            
            if self.docker_process.returncode != 0:
                self.logger.error("Failed to start Docker services", 
                                stderr=stderr)
                return False
            
            self.logger.info("Docker services started successfully")
            
            # Wait for services to be ready
            await self._wait_for_services()
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to start infrastructure", error=str(e))
            return False
    
    async def _wait_for_services(self) -> None:
        """Wait for all services to be ready."""
        services = [
            ("PostgreSQL", "localhost:5432"),
            ("Kafka", "localhost:9092"),
            ("Redis", "localhost:6379"),
            ("Spark Master", "localhost:8080"),
            ("Airflow", "localhost:8081")
        ]
        
        self.logger.info("Waiting for services to be ready...")
        
        for service_name, endpoint in services:
            max_retries = 30
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    # Simple connectivity check
                    if await self._check_service_health(endpoint):
                        self.logger.info(f"{service_name} is ready")
                        break
                    else:
                        retry_count += 1
                        await asyncio.sleep(2)
                        
                except Exception as e:
                    retry_count += 1
                    await asyncio.sleep(2)
            
            if retry_count >= max_retries:
                self.logger.warning(f"{service_name} may not be ready")
    
    async def _check_service_health(self, endpoint: str) -> bool:
        """Check if a service endpoint is healthy."""
        try:
            host, port = endpoint.split(':')
            # Simple TCP connection check
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, int(port)),
                timeout=5.0
            )
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False
    
    async def start_platform(self) -> bool:
        """Start the finance data platform."""
        try:
            self.logger.info("Starting Finance Data Platform...")
            
            # Initialize platform
            self.platform = FinanceDataPlatform()
            
            # Setup services
            await self.platform.setup_ingestion_services()
            
            # Start all services
            await self.platform.start_services()
            
            self.logger.info("Finance Data Platform started successfully")
            return True
            
        except Exception as e:
            self.logger.error("Failed to start platform", error=str(e))
            return False
    
    async def run_pipeline(self) -> None:
        """Run the complete pipeline."""
        try:
            self.is_running = True
            
            # Start infrastructure
            if not await self.start_infrastructure():
                self.logger.error("Failed to start infrastructure")
                return
            
            # Start platform
            if not await self.start_platform():
                self.logger.error("Failed to start platform")
                return
            
            self.logger.info("Pipeline is running! Press Ctrl+C to stop.")
            
            # Keep running until interrupted
            while self.is_running:
                await asyncio.sleep(1)
                
                # Check platform health
                if self.platform:
                    health = await self.platform.get_health_status()
                    if not health.get("overall_healthy", False):
                        self.logger.warning("Platform health degraded", health=health)
            
        except Exception as e:
            self.logger.error("Pipeline execution failed", error=str(e))
        
        finally:
            await self.cleanup()
    
    async def cleanup(self) -> None:
        """Cleanup resources and stop services."""
        try:
            self.logger.info("Cleaning up resources...")
            
            # Stop platform
            if self.platform:
                await self.platform.stop_services()
                self.logger.info("Platform stopped")
            
            # Stop Docker services
            if self.docker_process:
                self.logger.info("Stopping Docker services...")
                subprocess.run(["docker-compose", "down"], 
                             capture_output=True, text=True)
                self.logger.info("Docker services stopped")
            
            self.logger.info("Cleanup completed")
            
        except Exception as e:
            self.logger.error("Cleanup failed", error=str(e))
    
    def show_status(self) -> None:
        """Show current pipeline status."""
        print("\n" + "="*60)
        print("FINANCE DATA PIPELINE STATUS")
        print("="*60)
        
        # Check Docker services
        try:
            result = subprocess.run(
                ["docker-compose", "ps"],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print("Docker Services:")
                print(result.stdout)
            else:
                print("Docker services not running")
                
        except Exception as e:
            print(f"Error checking Docker status: {e}")
        
        print("\nService URLs:")
        print("- Airflow UI: http://localhost:8081 (admin/admin123)")
        print("- Spark Master: http://localhost:8080")
        print("- PostgreSQL: localhost:5432 (postgres/postgres)")
        print("- Kafka: localhost:9092")
        print("="*60)


async def main():
    """Main function."""
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
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "status":
            runner = PipelineRunner()
            runner.show_status()
            return
        elif command == "test":
            # Run tests
            print("Running pipeline tests...")
            subprocess.run([sys.executable, "test_complete_pipeline.py"])
            return
        elif command == "stop":
            # Stop services
            print("Stopping pipeline services...")
            subprocess.run(["docker-compose", "down"])
            return
        elif command == "help":
            print_help()
            return
    
    # Run the pipeline
    runner = PipelineRunner()
    
    try:
        await runner.run_pipeline()
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)


def print_help():
    """Print help information."""
    print("""
Finance Data Pipeline Runner

Usage:
    python run_pipeline.py [command]

Commands:
    (no command)    Start the complete pipeline
    status          Show current pipeline status
    test            Run pipeline tests
    stop            Stop all pipeline services
    help            Show this help message

Examples:
    python run_pipeline.py          # Start the pipeline
    python run_pipeline.py status   # Check status
    python run_pipeline.py test     # Run tests
    python run_pipeline.py stop     # Stop services

Service URLs:
    - Airflow UI: http://localhost:8081 (admin/admin123)
    - Spark Master: http://localhost:8080
    - PostgreSQL: localhost:5432 (postgres/postgres)
    - Kafka: localhost:9092
""")


if __name__ == "__main__":
    asyncio.run(main())
