"""
Monitoring and metrics collection utilities.

This module provides monitoring capabilities including metrics collection,
health checks, and observability features for the finance data platform.
"""

import time
from datetime import datetime
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
import threading
from collections import defaultdict, Counter

import structlog
from prometheus_client import Counter as PrometheusCounter, Gauge as PrometheusGauge, Histogram as PrometheusHistogram, start_http_server

from src.config.settings import settings


class MetricsCollector:
    """
    Metrics collection and monitoring utility.
    
    Provides a unified interface for collecting and exposing metrics
    for monitoring and observability.
    """
    
    def __init__(self):
        """Initialize the metrics collector."""
        self.logger = structlog.get_logger("MetricsCollector")
        
        # Initialize Prometheus metrics
        self._counters: Dict[str, PrometheusCounter] = {}
        self._gauges: Dict[str, PrometheusGauge] = {}
        self._histograms: Dict[str, PrometheusHistogram] = {}
        
        # Start Prometheus HTTP server if enabled
        if settings.monitoring.prometheus_port > 0:
            try:
                start_http_server(settings.monitoring.prometheus_port)
                self.logger.info("Prometheus metrics server started", port=settings.monitoring.prometheus_port)
            except Exception as e:
                self.logger.error("Failed to start Prometheus server", error=str(e))
    
    def _get_counter(self, name: str, description: str = "", labels: List[str] = None) -> PrometheusCounter:
        """Get or create a Prometheus counter."""
        if name not in self._counters:
            self._counters[name] = PrometheusCounter(
                name=name,
                documentation=description,
                labelnames=labels or []
            )
        return self._counters[name]
    
    def _get_gauge(self, name: str, description: str = "", labels: List[str] = None) -> PrometheusGauge:
        """Get or create a Prometheus gauge."""
        if name not in self._gauges:
            self._gauges[name] = PrometheusGauge(
                name=name,
                documentation=description,
                labelnames=labels or []
            )
        return self._gauges[name]
    
    def _get_histogram(self, name: str, description: str = "", labels: List[str] = None) -> PrometheusHistogram:
        """Get or create a Prometheus histogram."""
        if name not in self._histograms:
            self._histograms[name] = PrometheusHistogram(
                name=name,
                documentation=description,
                labelnames=labels or []
            )
        return self._histograms[name]
    
    def increment_counter(self, name: str, value: float = 1.0, labels: Dict[str, str] = None) -> None:
        """
        Increment a counter metric.
        
        Args:
            name: Metric name
            value: Value to increment by
            labels: Metric labels
        """
        try:
            counter = self._get_counter(name)
            if labels:
                counter.labels(**labels).inc(value)
            else:
                counter.inc(value)
        except Exception as e:
            self.logger.error("Failed to increment counter", name=name, error=str(e))
    
    def record_gauge(self, name: str, value: float, labels: Dict[str, str] = None) -> None:
        """
        Record a gauge metric value.
        
        Args:
            name: Metric name
            value: Gauge value
            labels: Metric labels
        """
        try:
            gauge = self._get_gauge(name)
            if labels:
                gauge.labels(**labels).set(value)
            else:
                gauge.set(value)
        except Exception as e:
            self.logger.error("Failed to record gauge", name=name, error=str(e))
    
    def record_histogram(self, name: str, value: float, labels: Dict[str, str] = None) -> None:
        """
        Record a histogram metric value.
        
        Args:
            name: Metric name
            value: Histogram value
            labels: Metric labels
        """
        try:
            histogram = self._get_histogram(name)
            if labels:
                histogram.labels(**labels).observe(value)
            else:
                histogram.observe(value)
        except Exception as e:
            self.logger.error("Failed to record histogram", name=name, error=str(e))
    
    @contextmanager
    def timer(self, name: str, labels: Dict[str, str] = None):
        """
        Context manager for timing operations.
        
        Args:
            name: Timer metric name
            labels: Metric labels
        """
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.record_histogram(name, duration, labels)


class HealthChecker:
    """
    Health checking utility for system components.
    
    Provides health check functionality for various system components
    and aggregates health status information.
    """
    
    def __init__(self):
        """Initialize the health checker."""
        self.logger = structlog.get_logger("HealthChecker")
        self.health_checks: Dict[str, callable] = {}
        self.last_check_results: Dict[str, Dict[str, Any]] = {}
    
    def register_health_check(self, name: str, check_func: callable) -> None:
        """
        Register a health check function.
        
        Args:
            name: Health check name
            check_func: Health check function that returns bool
        """
        self.health_checks[name] = check_func
        self.logger.info("Registered health check", name=name)
    
    def unregister_health_check(self, name: str) -> None:
        """
        Unregister a health check function.
        
        Args:
            name: Health check name
        """
        if name in self.health_checks:
            del self.health_checks[name]
            self.logger.info("Unregistered health check", name=name)
    
    async def run_health_checks(self) -> Dict[str, Dict[str, Any]]:
        """
        Run all registered health checks.
        
        Returns:
            Dictionary of health check results
        """
        results = {}
        
        for name, check_func in self.health_checks.items():
            try:
                start_time = time.time()
                
                # Run health check
                if hasattr(check_func, '__await__'):
                    # Async function
                    is_healthy = await check_func()
                else:
                    # Sync function
                    is_healthy = check_func()
                
                duration = time.time() - start_time
                
                result = {
                    "healthy": bool(is_healthy),
                    "timestamp": datetime.utcnow().isoformat(),
                    "duration": duration,
                    "error": None
                }
                
                if not is_healthy:
                    result["error"] = "Health check failed"
                
                results[name] = result
                self.last_check_results[name] = result
                
                self.logger.info("Health check completed", 
                               name=name,
                               healthy=is_healthy,
                               duration=duration)
                
            except Exception as e:
                result = {
                    "healthy": False,
                    "timestamp": datetime.utcnow().isoformat(),
                    "duration": None,
                    "error": str(e)
                }
                
                results[name] = result
                self.last_check_results[name] = result
                
                self.logger.error("Health check failed", 
                                name=name,
                                error=str(e))
        
        return results
    
    def get_overall_health(self) -> Dict[str, Any]:
        """
        Get overall system health status.
        
        Returns:
            Overall health status dictionary
        """
        if not self.last_check_results:
            return {
                "overall": "unknown",
                "healthy_count": 0,
                "unhealthy_count": 0,
                "total_count": 0,
                "last_check": None
            }
        
        healthy_count = sum(1 for result in self.last_check_results.values() if result["healthy"])
        total_count = len(self.last_check_results)
        unhealthy_count = total_count - healthy_count
        
        # Determine overall health
        if unhealthy_count == 0:
            overall = "healthy"
        elif healthy_count == 0:
            overall = "unhealthy"
        else:
            overall = "degraded"
        
        return {
            "overall": overall,
            "healthy_count": healthy_count,
            "unhealthy_count": unhealthy_count,
            "total_count": total_count,
            "last_check": max(result["timestamp"] for result in self.last_check_results.values()),
            "checks": self.last_check_results
        }


class PerformanceMonitor:
    """
    Performance monitoring utility.
    
    Tracks performance metrics and provides insights into system performance.
    """
    
    def __init__(self):
        """Initialize the performance monitor."""
        self.logger = structlog.get_logger("PerformanceMonitor")
        self.metrics = MetricsCollector()
        self.operation_times: Dict[str, List[float]] = defaultdict(list)
        self.error_counts: Counter = Counter()
        self.success_counts: Counter = Counter()
        self._lock = threading.Lock()
    
    def record_operation(self, operation_name: str, duration: float, success: bool = True, 
                        labels: Dict[str, str] = None) -> None:
        """
        Record operation performance metrics.
        
        Args:
            operation_name: Name of the operation
            duration: Operation duration in seconds
            success: Whether operation was successful
            labels: Additional labels for metrics
        """
        with self._lock:
            self.operation_times[operation_name].append(duration)
            
            # Keep only last 1000 measurements
            if len(self.operation_times[operation_name]) > 1000:
                self.operation_times[operation_name] = self.operation_times[operation_name][-1000:]
        
        # Record Prometheus metrics
        metric_labels = labels or {}
        metric_labels["operation"] = operation_name
        
        self.metrics.record_histogram("operation_duration", duration, metric_labels)
        
        if success:
            self.success_counts[operation_name] += 1
            self.metrics.increment_counter("operation_success", labels=metric_labels)
        else:
            self.error_counts[operation_name] += 1
            self.metrics.increment_counter("operation_errors", labels=metric_labels)
    
    @contextmanager
    def monitor_operation(self, operation_name: str, labels: Dict[str, str] = None):
        """
        Context manager for monitoring operations.
        
        Args:
            operation_name: Name of the operation
            labels: Additional labels for metrics
        """
        start_time = time.time()
        success = True
        
        try:
            yield
        except Exception as e:
            success = False
            self.logger.error("Operation failed", 
                            operation=operation_name,
                            error=str(e))
            raise
        finally:
            duration = time.time() - start_time
            self.record_operation(operation_name, duration, success, labels)
    
    def get_operation_stats(self, operation_name: str) -> Dict[str, Any]:
        """
        Get performance statistics for an operation.
        
        Args:
            operation_name: Name of the operation
            
        Returns:
            Performance statistics dictionary
        """
        if operation_name not in self.operation_times:
            return {
                "count": 0,
                "avg_duration": 0,
                "min_duration": 0,
                "max_duration": 0,
                "success_rate": 0
            }
        
        times = self.operation_times[operation_name]
        total_count = len(times)
        success_count = self.success_counts[operation_name]
        error_count = self.error_counts[operation_name]
        
        if total_count == 0:
            return {
                "count": 0,
                "avg_duration": 0,
                "min_duration": 0,
                "max_duration": 0,
                "success_rate": 0
            }
        
        return {
            "count": total_count,
            "avg_duration": sum(times) / len(times),
            "min_duration": min(times),
            "max_duration": max(times),
            "success_rate": success_count / (success_count + error_count) if (success_count + error_count) > 0 else 0,
            "success_count": success_count,
            "error_count": error_count
        }
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get performance statistics for all operations.
        
        Returns:
            Dictionary of operation statistics
        """
        return {
            operation: self.get_operation_stats(operation)
            for operation in self.operation_times.keys()
        }


# Global instances
metrics_collector = MetricsCollector()
health_checker = HealthChecker()
performance_monitor = PerformanceMonitor()
