# Airline Analytics & Real-Time Anomaly Detection Platform

A comprehensive data pipeline platform for airline analytics, combining batch processing of historical flight data with real-time streaming for anomaly detection. This project was developed as part of the Samsung Innovation Campus x Life Makers program.

## 🚀 Project Overview

The platform processes airline on-time performance data to provide:
- **Historical Analytics**: Batch processing of 116+ million flight records (1987-2008)
- **Real-Time Monitoring**: Streaming pipeline for immediate delay detection
- **Machine Learning**: Predictive models for flight delay forecasting
- **Operational Dashboards**: Business intelligence visualizations

## 📊 Architecture

The system follows a dual-path workflow:

### **Batch Layer** (Historical Processing)

Kaggle Dataset → AWS S3 → Snowflake → dbt Transform → ML Models → Dashboard

### **Speed Layer** (Real-Time Processing)

Snowflake → Kafka → Spark Streaming → Anomaly Detection → Real-Time Alerts


## 🏗️ Technical Stack

| Component | Technology |
|-----------|------------|
| **Data Storage** | AWS S3, Snowflake |
| **Data Processing** | dbt, Apache Spark |
| **Streaming** | Apache Kafka, Spark Structured Streaming |
| **Orchestration** | Apache Airflow |
| **Machine Learning** | scikit-learn, Snowpark |
| **Visualization** | Microsoft Power BI |

## 🛠️ Setup & Installation

### Prerequisites
- Python 3.8+
- AWS Account with S3 access
- Snowflake Account
- Docker (for Kafka/Spark)
- dbt Cloud or CLI

### 1. Data Ingestion
```
# Install dependencies
pip install kaggle boto3 pandas

# Configure AWS credentials
aws configure

# Run data extraction
python data_ingestion/kaggle_to_s3.py
```
### 2. Snowflake Setup
```
-- Create database and schema
CREATE DATABASE AIRLINE_PROJECT;
USE DATABASE AIRLINE_PROJECT;
CREATE SCHEMA RAW;

-- Create external stage
CREATE OR REPLACE STAGE airline_s3_stage
    URL = 's3://airline-dataset-1/raw-data/'
    CREDENTIALS = (AWS_KEY_ID='xxx', AWS_SECRET_KEY='xxx');
```
### 3. dbt Transformation
```
# Initialize dbt project
dbt init airline_project

# Run transformations
dbt run --models stg_flights dim_* fact_flights
dbt test
```
### 4. Real-Time Streaming
```
# Start Kafka and Spark services
docker-compose up -d

# Run producer (Snowflake to Kafka)
python streaming/stream_to_kafka.py

# Run stream processor
python streaming/spark_processor.py
```
## 📈 Key Features

### Data Pipeline
Extraction from Kaggle API

Secure S3 storage with IAM validation

Snowflake data warehousing with external stages

Incremental loading with COPY INTO commands

### Data Transformation
Medallion architecture implementation (Bronze→Silver→Gold)

Dimension and fact table modeling

Comprehensive data quality tests

Feature engineering for ML

### Machine Learning
Random Forest Regressor for delay prediction

Feature engineering with rolling windows

Model training within Snowflake using Snowpark

Performance metrics: MAE=18.76, RMSE=33.11

### Real-Time Processing
Snowflake to Kafka producer

Spark Structured Streaming processing

Anomaly detection (>60 minute delays)

Real-time alerting system

## 📊 Dashboard Insights
The platform provides dashboards with key metrics:

Overall On-Time Performance: 80.68%

Average Arrival Delay: 7.07 minutes

Total Flights Analyzed: 116+ million

Seasonal Trends: Peak travel in March, July, December

Carrier Performance: Delta leads in volume and punctuality

## 🚨 Real-Time Alerting
The streaming pipeline detects high-delay anomalies (>60 minutes) and generates alerts with:

Flight identification details

Delay duration

Route information

Timestamp of detection

![Airline Analytics](https://github.com/user-attachments/assets/65d7eb7c-8101-44e6-8d67-42dc8b35605c)

## 🔧 Configuration
Environment Variables
```
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export SNOWFLAKE_ACCOUNT=your_account
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```
dbt Profiles
```
airline_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account
      user: your_user
      password: your_password
      role: ACCOUNTADMIN
      warehouse: COMPUTE_WH
      database: AIRLINE_PROJECT
      schema: raw
```

## 🚀 Performance
Data Loading: 116+ million records in single COPY operation

Stream Processing: Real-time with <1 second latency

ML Training: 100K sample training in minutes

Query Performance: Optimized with Snowflake clustering

## 🔮 Future Enhancements

1. **Enhanced ML Models**  
   - Deep learning for delay prediction  
   - Training on full 116M row dataset for improved accuracy  
   - Explainable AI features for operational insights

2. **Advanced Anomaly Detection**  
   - Pattern recognition for cascading delays  
   - Multi-variate analysis with weather/maintenance data  
   - Predictive alerts before delays occur

3. **Predictive Maintenance**  
   - Aircraft maintenance scheduling optimization  
   - Component failure prediction models  
   - Cost optimization through planned maintenance

4. **Dynamic Pricing**  
   - Fare optimization based on demand predictions  
   - Customer behavior modeling  
   - Revenue management integration

5. **Real-Time Integration**  
   - Full streaming pipeline with Kafka-Spark  
   - Instant predictions for live flight data  
   - Mobile push notifications for passengers

6. **Operational Applications**  
   - Airline scheduling optimization  
   - Proactive customer communication  
   - Airport operations management

## 👥 Team

This project was developed by **Yasmina**, **Nancy**, and **Naser** as part of the Samsung Innovation Campus x Life Makers Big Data graduation program.

## 📄 License & Data Attribution

This project is developed for **educational purposes** as part of the Samsung Innovation Campus program.

### Dataset Source
The project uses the **[Airline On-Time Performance dataset](https://www.kaggle.com/datasets/ahmedelsayedrashad/airline-on-time-performance-data)** from Kaggle for academic demonstration purposes.

*Note: All data processing and analysis shown here is for educational demonstration within the Samsung Innovation Campus program.*
