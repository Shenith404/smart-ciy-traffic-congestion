import json
import time
import random
import datetime
import threading
import sys
from kafka import KafkaProducer

# Configuration
KAFKA_TOPIC = 'traffic_data'
JUNCTIONS = ['J001', 'J002', 'J003', 'J004']
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

traffic_jam_active = False

def input_listener():
    """Listens for user input to trigger a traffic jam demo."""
    global traffic_jam_active
    while True:
        input("Press Enter to trigger CRITICAL TRAFFIC JAM (Demo Mode)...")
        traffic_jam_active = True
        print("!!! TRAFFIC JAM TRIGGERED FOR NEXT 10 SECONDS !!!")
        time.sleep(10)
        traffic_jam_active = False
        print("... Traffic returning to normal.")

# Start input listener in a separate thread
threading.Thread(target=input_listener, daemon=True).start()

print(f"Producer started. Sending data to {KAFKA_TOPIC}...")

try:
    while True:
        for junction in JUNCTIONS:
            # Normal Logic
            speed = random.uniform(30, 80)
            count = random.randint(5, 50)
            
            # DEMO LOGIC: If triggered, drop speed to < 10 [cite: 45]
            if traffic_jam_active and junction == 'J001':
                speed = random.uniform(2, 9) 
                count = random.randint(80, 120) # High congestion

            data = {
                'sensor_id': junction,
                'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'vehicle_count': count,
                'avg_speed': round(speed, 2)
            }
            
            producer.send(KAFKA_TOPIC, data)
            print(f"Sent: {data}")
            
        time.sleep(1) # Send every second [cite: 38]

except KeyboardInterrupt:
    print("Producer stopped.")