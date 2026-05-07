-- Table for Real-Time Alerts (Low Speed < 10 km/h)
CREATE TABLE IF NOT EXISTS critical_alerts (
    sensor_id VARCHAR(50),
    timestamp VARCHAR(50),
    vehicle_count INTEGER,
    avg_speed FLOAT,
    congestion_index FLOAT,
    alert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Daily Reports (Peak Hours per Junction)
CREATE TABLE IF NOT EXISTS peak_traffic_stats (
    junction_id VARCHAR(50),
    peak_hour INTEGER,
    max_vehicle_count BIGINT,
    report_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for Windowed Aggregations (5-minute windows)
CREATE TABLE IF NOT EXISTS traffic_windows (
    sensor_id VARCHAR(50),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_vehicles BIGINT,
    avg_speed_window FLOAT,
    congestion_index FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);