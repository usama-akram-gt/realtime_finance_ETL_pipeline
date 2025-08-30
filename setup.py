#!/usr/bin/env python3
"""
Finance Data Pipeline Setup Script
Sets up complete production pipeline with Airflow, Spark, DBT, and Great Expectations
"""

import subprocess
import sys
import os
import time
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

def print_header():
    """Print setup header."""
    console.print(Panel.fit(
        "Finance Data Pipeline Setup\n"
        "Airflow + Spark + DBT + Great Expectations + Kafka + PostgreSQL",
        style="blue"
    ))

def check_prerequisites():
    """Check if required tools are installed."""
    console.print("Checking prerequisites...", style="yellow")
    
    tools = [
        ("Docker", "docker --version"),
        ("Docker Compose", "docker-compose --version"),
        ("Python", "python3 --version"),
        ("pip", "pip --version"),
    ]
    
    missing = []
    for tool, command in tools:
        try:
            result = subprocess.run(command.split(), capture_output=True, text=True)
            if result.returncode == 0:
                console.print(f"[OK] {tool}: {result.stdout.strip()}", style="green")
            else:
                missing.append(tool)
        except FileNotFoundError:
            missing.append(tool)
    
    if missing:
        console.print(f"[ERROR] Missing tools: {', '.join(missing)}", style="red")
        return False
    
    return True

def install_python_dependencies():
    """Install Python dependencies."""
    console.print("Installing Python dependencies...", style="yellow")
    
    try:
        subprocess.run([
            sys.executable, "-m", "pip", "install", "-r", "requirements.txt"
        ], check=True)
        console.print("[OK] Python dependencies installed", style="green")
        return True
    except subprocess.CalledProcessError as e:
        console.print(f"[ERROR] Failed to install dependencies: {e}", style="red")
        return False

def start_infrastructure():
    """Start infrastructure services."""
    console.print("Starting infrastructure services...", style="yellow")
    
    try:
        # Stop any existing services
        subprocess.run([
            "docker-compose", "down"
        ], capture_output=True)
        
        # Start services
        subprocess.run([
            "docker-compose", "up", "-d"
        ], check=True)
        
        console.print("[OK] Infrastructure services started", style="green")
        return True
    except subprocess.CalledProcessError as e:
        console.print(f"[ERROR] Failed to start services: {e}", style="red")
        return False

def wait_for_services():
    """Wait for all services to be ready."""
    console.print("⏳ Waiting for services to be ready...", style="yellow")
    
    services = [
        ("Zookeeper", "localhost:2181"),
        ("Kafka", "localhost:9092"),
        ("PostgreSQL", "localhost:5432"),
        ("Redis", "localhost:6379"),
        ("Spark Master", "localhost:8080"),
        ("Airflow Webserver", "localhost:8081"),
    ]
    
    for service_name, endpoint in services:
        console.print(f"  Waiting for {service_name}...", style="yellow")
        max_attempts = 60
        for attempt in range(max_attempts):
            try:
                if ":" in endpoint:
                    host, port = endpoint.split(":")
                    import socket
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result = sock.connect_ex((host, int(port)))
                    sock.close()
                    if result == 0:
                        console.print(f"  [OK] {service_name} is ready", style="green")
                        break
                time.sleep(5)
            except:
                pass
        else:
            console.print(f"  [WARNING] {service_name} may not be ready", style="yellow")

def setup_airflow():
    """Initialize Airflow."""
    console.print("Setting up Airflow...", style="yellow")
    
    try:
        # Wait for Airflow to be ready
        time.sleep(30)
        
        # Initialize Airflow database
        subprocess.run([
            "docker", "exec", "airflow-webserver", 
            "airflow", "db", "init"
        ], check=True)
        
        # Create admin user
        subprocess.run([
            "docker", "exec", "airflow-webserver",
            "airflow", "users", "create",
            "--username", "admin",
            "--firstname", "Admin",
            "--lastname", "User",
            "--role", "Admin",
            "--email", "admin@finance-data.com",
            "--password", "admin123"
        ], check=True)
        
        console.print("[OK] Airflow initialized", style="green")
        return True
    except subprocess.CalledProcessError as e:
        console.print(f"[ERROR] Airflow setup failed: {e}", style="red")
        return False

def setup_dbt():
    """Initialize DBT project."""
    console.print("Setting up DBT...", style="yellow")
    
    try:
        # Create .dbt directory
        os.makedirs(".dbt", exist_ok=True)
        
        # Copy profiles.yml to .dbt directory
        if os.path.exists("src/dbt/profiles.yml"):
            subprocess.run(["cp", "src/dbt/profiles.yml", ".dbt/"], check=True)
            console.print("[OK] DBT profiles configured", style="green")
        
        # Test DBT connection from dbt directory
        subprocess.run(["dbt", "debug"], cwd="src/dbt", check=True)
        console.print("[OK] DBT project initialized", style="green")
        return True
        
    except subprocess.CalledProcessError as e:
        console.print(f"[ERROR] DBT setup failed: {e}", style="red")
        return False

def setup_great_expectations():
    """Initialize Great Expectations."""
    console.print("Setting up Great Expectations...", style="yellow")
    
    try:
        subprocess.run(["great_expectations", "init"], cwd="src/great_expectations", check=True)
        console.print("[OK] Great Expectations initialized", style="green")
        return True
    except subprocess.CalledProcessError as e:
        console.print(f"[ERROR] Great Expectations setup failed: {e}", style="red")
        return False

def test_pipeline():
    """Test the complete pipeline."""
    console.print("Testing pipeline...", style="yellow")
    
    try:
        # Run the test pipeline
        subprocess.run([
            sys.executable, "test_pipeline.py"
        ], check=True)
        console.print("[OK] Pipeline test completed", style="green")
        return True
    except subprocess.CalledProcessError as e:
        console.print(f"[ERROR] Pipeline test failed: {e}", style="red")
        return False

def show_service_status():
    """Show service status."""
    try:
        result = subprocess.run([
            "docker-compose", "ps"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            console.print("\nService Status:", style="blue")
            console.print(result.stdout)
    except:
        pass

def show_urls():
    """Show service URLs."""
    table = Table(title="Service URLs")
    table.add_column("Service", style="cyan")
    table.add_column("URL", style="green")
    table.add_column("Credentials", style="yellow")
    table.add_column("Description", style="magenta")
    
    table.add_row("Apache Airflow", "http://localhost:8081", "admin/admin123", "Workflow Orchestration")
    table.add_row("Spark Master", "http://localhost:8080", "No auth", "Spark Cluster Management")
    table.add_row("Kafka", "localhost:9092", "No auth", "Message Streaming")
    table.add_row("PostgreSQL", "localhost:5432", "postgres/postgres", "Data Storage")
    table.add_row("Redis", "localhost:6379", "No auth", "Caching")
    table.add_row("Zookeeper", "localhost:2181", "No auth", "Kafka Coordination")
    
    console.print(table)

def show_next_steps():
    """Show next steps for the user."""
    console.print("\nSetup Complete! Next Steps:", style="green")
    
    table = Table(title="Next Steps")
    table.add_column("Step", style="cyan")
    table.add_column("Command", style="green")
    table.add_column("Description", style="yellow")
    
    table.add_row(
        "1. Install TimescaleDB",
        "Follow TimescaleDB docs",
        "Install TimescaleDB extension in PostgreSQL"
    )
    table.add_row(
        "2. Access Airflow",
        "http://localhost:8081",
        "Login with admin/admin123 and trigger DAGs"
    )
    table.add_row(
        "3. Run DBT Models",
        "dbt run",
        "Transform raw data into analytics-ready tables"
    )
    table.add_row(
        "4. Test Spark Jobs",
        "spark-submit",
        "Submit Spark jobs for data processing"
    )
    table.add_row(
        "5. Monitor Pipeline",
        "Airflow UI",
        "Monitor DAG execution and data quality"
    )
    table.add_row(
        "6. View Data",
        "dbeaver",
        "Connect to PostgreSQL to view processed data"
    )
    
    console.print(table)

def main():
    """Main setup function."""
    print_header()
    
    if not check_prerequisites():
        console.print("[ERROR] Prerequisites not met. Please install missing tools.", style="red")
        return
    
    if not install_python_dependencies():
        console.print("[ERROR] Failed to install Python dependencies.", style="red")
        return
    
    if not start_infrastructure():
        console.print("[ERROR] Failed to start infrastructure.", style="red")
        return
    
    wait_for_services()
    
    if not setup_airflow():
        console.print("[ERROR] Failed to setup Airflow.", style="red")
        return
    
    if not setup_dbt():
        console.print("[ERROR] Failed to setup DBT.", style="red")
        return
    
    if not setup_great_expectations():
        console.print("[ERROR] Failed to setup Great Expectations.", style="red")
        return
    
    if not test_pipeline():
        console.print("[ERROR] Pipeline test failed.", style="red")
        return
    
    show_service_status()
    show_urls()
    show_next_steps()

if __name__ == "__main__":
    main()
