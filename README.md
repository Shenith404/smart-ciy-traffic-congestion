# Smart City Traffic Congestion Monitoring System

## Colombo Traffic Management - Lambda Architecture Implementation
---

###  Project Overview

This project implements an end-to-end **Lambda Architecture** data pipeline for real-time traffic monitoring in Colombo, Sri Lanka. The system processes sensor data from 4 major junctions to detect congestion, generate alerts, and provide daily analytical reports for traffic management.

---

## Architecture

### Lambda Architecture Components

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                            │
│              (4 Junctions: J001, J002, J003, J004)              │
└──────────────┬──────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────┐
│                      SPEED LAYER (Real-time)                     │
│  ┌────────────┐    ┌─────────────┐    ┌──────────────────┐       │
│  │   Kafka    │───▶│Spark Stream │───▶│  PostgreSQL     │       │
│  │  Producer  │    │  Processor  │    │  (Alerts & Data) │       │
│  └────────────┘    └─────────────┘    └──────────────────┘       │
│                           │                                      │
│                           ▼                                      │
│                    ┌─────────────┐                               │
│                    │   Parquet   │  (Data Lake)                  │
│                    └─────────────┘                               │
└──────────────────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────────────┐
│                      BATCH LAYER (Historical)                    │
│  ┌────────────┐    ┌─────────────┐    ┌──────────────────┐       │
│  │  Airflow   │───▶│Spark Batch  │───▶│  PostgreSQL     │       │
│  │   (DAG)    │    │  Analyzer   │    │  (Peak Reports)  │       │
│  └────────────┘    └─────────────┘    └──────────────────┘       │
│                           │                                      │
│                           ▼                                      │
│                    ┌─────────────┐                               │
│                    │Visualization│                               │
│                    └─────────────┘                               │
└──────────────────────────────────────────────────────────────────┘
```

---

## Technical Stack

| Component             | Technology                          |
| --------------------- | ----------------------------------- |
| **Data Ingestion**    | Apache Kafka                        |
| **Stream Processing** | Apache Spark (Structured Streaming) |
| **Orchestration**     | Apache Airflow                      |
| **Storage**           | PostgreSQL + Parquet (Data Lake)    |
| **Visualization**     | Matplotlib + Seaborn                |
| **Containerization**  | Docker Compose                      |

---

## Project Structure

```
smart-city-traffic-congestion/
├── docker-compose.yaml       # All services (Kafka, Postgres, Airflow)
├── init.sql                  # Database schema initialization
├── requirements.txt          # Python dependencies
├── dags/
│   └── traffic_dag.py       # Airflow DAG for nightly batch jobs
├── src/
│   ├── producer.py          # Mock sensor data generator (Kafka producer)
│   ├── spark_processor.py   # Real-time stream processor with windowing
│   ├── batch_analizer.py    # Daily batch job for peak traffic analysis
│   └── visualize_traffic.py # Report generation script
├── data/
│   ├── traffic_history/     # Parquet files (generated)
│   ├── checkpoint*/         # Spark checkpoints (generated)
│   └── reports/             # Generated visualizations
```

---

## Setup Instructions

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- 8GB RAM minimum
- 10GB free disk space

### Step 1: Clone and Navigate

```bash
cd smart-city-traffic-congestion
```

### Step 2: Start Infrastructure

```bash
docker-compose up -d
```

This starts:

- **Kafka** (port 9092) - KRaft mode (no Zookeeper needed)
- **PostgreSQL** (port 5432)
- **Airflow** (port 8080)

Wait 30-60 seconds for all services to be ready.

> **Note:** Using Apache Kafka 3.7.0 in KRaft mode (Kafka Raft metadata mode) which doesn't require Zookeeper.

### Step 3: Verify Services

```bash
# Check all containers are running
docker-compose ps

# Check PostgreSQL tables were created
docker exec -it <postgres_container_id> psql -U admin -d smartcity_db -c "\dt"
```

Expected tables: `critical_alerts`, `peak_traffic_stats`, `traffic_windows`

### Step 4: Install Python Dependencies (Local Execution)

```bash
python -m venv venv
venv\Scripts\activate      # Windows
# source venv/bin/activate  # Mac/Linux

pip install -r requirements.txt
```

---

##  Running the System

### Option A: Manual Execution (Recommended for Testing)

#### 1. Start Kafka Producer (Sensor Simulator)

```bash
python src/producer.py
```

- Sends data every second from 4 junctions
- **Press Enter** to trigger critical traffic jam (speed < 10 km/h) for demo

#### 2. Start Spark Streaming Processor

```bash
python src/spark_processor.py
```

This will:

-  Apply 5-minute tumbling windows
-  Calculate **Congestion Index** = `(vehicle_count / avg_speed) × 10`
-  Detect critical traffic (speed < 10 km/h)
-  Write alerts to PostgreSQL `critical_alerts` table
-  Archive raw data to Parquet for batch processing

#### 3. Trigger Airflow DAG (Batch Processing)

**Via Airflow UI:**

1. Open http://localhost:8080
2. Login: `admin` / `admin`
3. Enable DAG: `smart_city_daily_report`
4. Click "Trigger DAG" (play button)

**Via Command Line:**

```bash
docker exec -it <airflow_container_id> airflow dags trigger smart_city_daily_report
```

The batch job will:

-  Load yesterday's data from Parquet
-  Calculate peak traffic hour per junction
-  Generate intervention recommendations
-  Write report to PostgreSQL `peak_traffic_stats`
-  Create visualization charts

#### 4. View Results

**Check Critical Alerts:**

```sql
docker exec -it <postgres_container_id> psql -U admin -d smartcity_db
SELECT * FROM critical_alerts ORDER BY timestamp DESC LIMIT 10;
```

**Check Peak Traffic Reports:**

```sql
SELECT * FROM peak_traffic_stats ORDER BY report_date DESC;
```

**View Visualizations:**

```
data/reports/traffic_report_latest.png
```

**View Analyzed Report Output (CSV):**

```
data/reports/traffic_report_latest.csv
```

---

##  Key Features Implemented

### 1. Real-Time Stream Processing (Speed Layer)

-  **5-Minute Tumbling Windows** with watermarking
-  **Congestion Index** calculation per window
-  Immediate alert on `avg_speed < 10 km/h`
-  Windowed aggregations stored in `traffic_windows` table

### 2. Batch Processing (Batch Layer)

-  Nightly Airflow DAG scheduled at 2 AM
-  Processes previous day's data from Parquet lake
-  Identifies **Peak Traffic Hour** per junction
-  Generates intervention recommendations

### 3. Data Storage

-  **PostgreSQL**: Real-time alerts, batch reports, windowed aggregations
-  **Parquet**: Data lake for historical analysis (partitioned by sensor_id)

### 4. Analytics & Visualization

-  Traffic Volume vs. Time of Day (line chart)
-  Average traffic by junction (bar chart)
-  Traffic intensity heatmap
-  Peak traffic statistics table
-  CSV analyzed report output 

---

##  Testing Critical Traffic Scenario

1. Start producer: `python src/producer.py`
2. Start stream processor: `python src/spark_processor.py`
3. In producer terminal, **press Enter** to trigger jam
4. Watch for output:
   ```
   !!! TRAFFIC JAM TRIGGERED FOR NEXT 10 SECONDS !!!
   ```
5. In spark_processor terminal, you'll see:
   ```
   [Batch X] ALERT! Written N critical traffic records (speed < 10 km/h)
   ```
6. Query database to verify alerts

---

##  Performance Tuning

### For Large Data Volumes:

```python
# In spark_processor.py, add:
.config("spark.sql.shuffle.partitions", "4")
.config("spark.streaming.kafka.maxRatePerPartition", "1000")
```

### For Faster Batch Processing:

```python
# In batch_analizer.py, add:
.config("spark.sql.adaptive.enabled", "true")
```

---

##  Troubleshooting

### Issue: Kafka Connection Refused

```bash
# Verify Kafka is running
docker logs <kafka_container_id>

# Check Kafka topic
docker exec -it <kafka_container_id> kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Issue: Spark Can't Write to PostgreSQL

```bash
# Ensure PostgreSQL is accessible
docker exec -it <postgres_container_id> psql -U admin -d smartcity_db -c "SELECT 1;"

# Check init.sql was executed
docker exec -it <postgres_container_id> psql -U admin -d smartcity_db -c "\dt"
```

### Issue: No Data in Visualizations

- Ensure producer and spark_processor ran for at least 10 minutes
- Check Parquet files exist: `ls data/traffic_history/`
- Run batch analyzer manually: `python src/batch_analizer.py`

---

##  Data Schema

### Kafka Message Format

```json
{
  "sensor_id": "J001",
  "timestamp": "2024-01-15 13:45:30",
  "vehicle_count": 45,
  "avg_speed": 25.5
}
```

### PostgreSQL Tables

#### critical_alerts

```sql
sensor_id       | VARCHAR(50)
timestamp       | VARCHAR(50)
vehicle_count   | INTEGER
avg_speed       | FLOAT
congestion_index| FLOAT
alert_time      | TIMESTAMP (auto)
```

#### traffic_windows

```sql
sensor_id       | VARCHAR(50)
window_start    | TIMESTAMP
window_end      | TIMESTAMP
total_vehicles  | BIGINT
avg_speed_window| FLOAT
congestion_index| FLOAT
```

#### peak_traffic_stats

```sql
junction_id     | VARCHAR(50)
peak_hour       | INTEGER (0-23)
max_vehicle_count| BIGINT
report_date     | DATE
```

---

##  License

MIT License - Academic Use

---

##  Support

For issues:

1. Check logs: `docker-compose logs <service_name>`
2. Verify all services: `docker-compose ps`
3. Restart services: `docker-compose restart`

---


