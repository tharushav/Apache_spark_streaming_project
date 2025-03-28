import time
import pandas as pd
import os
import numpy as np
import random
from datetime import datetime

# Load the dataset
data = pd.read_csv('/Users/tharushavihanga/Developer/spark_mongo/modified_file.csv')

# Directory for simulated data
output_dir = "/Users/tharushavihanga/Developer/spark_mongo/stream_data"
os.makedirs(output_dir, exist_ok=True)

# Clean old files if they exist
for file in os.listdir(output_dir):
    if file.endswith(".csv"):
        os.remove(os.path.join(output_dir, file))

print(f"Starting data stream simulation at {datetime.now()}")

# Define functions to introduce occasional anomalies
def introduce_work_hours_anomaly(row):
    # 5% chance of anomaly
    if random.random() < 0.05:
        # Generate an outlier value for hours_per_week
        return random.choice([90, 95, 100, 5, 3, 1])
    return row['hours_per_week']

# Simulate streaming for 500 rows (more data for better visualization)
count = 0
while count < 500:
    # Select a random batch of rows (3-10 rows per batch)
    batch_size = random.randint(3, 10)
    batch = data.sample(n=batch_size)
    
    # Introduce occasional anomalies
    batch['hours_per_week'] = batch.apply(introduce_work_hours_anomaly, axis=1)
    
    # Write batch to file
    timestamp = int(time.time())
    file_path = os.path.join(output_dir, f"batch_{timestamp}_{count}.csv")
    batch.to_csv(file_path, index=False, header=False)
    
    # Increment counter
    count += batch_size
    
    # Print progress
    if count % 50 == 0:
        print(f"Generated {count} records at {datetime.now()}")
    
    # Sleep for 10 seconds between batches
    time.sleep(10)

print(f"Completed data stream simulation at {datetime.now()}")
print(f"Total records generated: {count}")
