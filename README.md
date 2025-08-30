# 🚀 Real-Time Finance ETL Pipeline

A production-ready real-time financial data pipeline built with modern data engineering technologies for enterprise-scale processing and analytics.

## 📊 Pipeline Walkthrough

### **Data Flow Architecture**
```
📈 Financial APIs → 🔄 Apache Kafka → ⚡ Apache Spark → 🗄️ PostgreSQL → 📋 DBT → 📊 Analytics
```

### **Step-by-Step Process**

1. **📥 Data Ingestion**
   - **Yahoo Finance API** fetches real-time market data
   - **Python async services** handle rate limiting and error recovery
   - **Structured logging** tracks all ingestion activities

2. **🔄 Message Streaming** 
   - **Apache Kafka** queues financial data for reliable processing
   - **Zookeeper** manages Kafka cluster coordination
   - **Real-time data flow** ensures low-latency processing

3. **⚡ Data Processing**
   - **Apache Spark** processes streaming data in real-time
   - **Technical indicators** (RSI, MACD, Moving Averages)
   - **Anomaly detection** and volatility calculations

4. **🗄️ Data Storage**
   - **PostgreSQL** stores processed financial data
   - **Optimized schemas** for time-series analysis
   - **Redis caching** for high-frequency queries

5. **🔧 Orchestration**
   - **Apache Airflow** manages pipeline workflows
   - **Automated scheduling** every 15 minutes
   - **Error handling** and retry mechanisms

6. **📋 Data Transformation**
   - **DBT models** transform raw data into analytics tables
   - **Staging → Marts** data modeling approach
   - **Data quality tests** ensure reliability

7. **📊 Analytics & Monitoring**
   - **Real-time dashboards** for market insights
   - **Prometheus metrics** for system monitoring
   - **Structured alerts** for anomalies

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Orchestration** | Apache Airflow | Workflow management & scheduling |
| **Streaming** | Apache Kafka + Zookeeper | Message queuing & real-time data flow |
| **Processing** | Apache Spark | Distributed data processing |
| **Storage** | PostgreSQL + Redis | Data persistence & caching |
| **Transformation** | DBT | Data modeling & transformations |
| **Quality** | Great Expectations | Data validation & testing |
| **APIs** | Yahoo Finance | Financial market data source |
| **Infrastructure** | Docker + Docker Compose | Containerized deployment |

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.13+
- 8GB+ RAM recommended

### Setup & Run
```bash
# 1. Clone repository
git clone https://github.com/usama-akram-gt/realtime_finance_ETL_pipeline.git
cd realtime_finance_ETL_pipeline

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start infrastructure
docker-compose up -d

# 5. Test pipeline
python3 test_pipeline.py
```

## 📈 Access Services

- **Airflow UI**: http://localhost:8081 (admin/admin123)
- **Spark Master**: http://localhost:8080  
- **PostgreSQL**: localhost:5432 (postgres/postgres)
- **Kafka**: localhost:9092

## 🏗️ Project Structure

```
src/
├── ingestion/          # Data ingestion services
├── streaming/          # Kafka streaming components  
├── spark_jobs/         # Spark processing jobs
├── storage/            # Database operations
├── dbt/                # DBT transformations
├── airflow/            # Workflow orchestration
└── great_expectations/ # Data quality checks
```

## 📊 Key Features

- **⚡ Real-time Processing**: Sub-second data ingestion and processing
- **🔄 Fault Tolerance**: Automatic retry and error recovery mechanisms  
- **📈 Scalability**: Horizontally scalable architecture
- **🛡️ Data Quality**: Comprehensive validation and testing
- **📊 Monitoring**: Full observability with metrics and logging
- **🏭 Production Ready**: Enterprise-grade coding practices

## 💡 Use Cases

- **Trading Analytics**: Real-time market analysis and decision support
- **Risk Management**: Portfolio risk assessment and monitoring
- **Market Research**: Historical and real-time market trend analysis
- **Algorithmic Trading**: Data feed for trading algorithms

Built with ❤️ for modern data engineering