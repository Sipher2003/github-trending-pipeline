from kafka import KafkaConsumer
import boto3, json, time
from config import *

s3 = boto3.client('s3', region_name=AWS_REGION)
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

if __name__ == "__main__":
    count = 0
    print("Listening for messages... (Ctrl+C to stop)")
    for msg in consumer:
        count += 1
        filename = f"raw-data/github_{int(time.time())}_{count}.json"
        s3.put_object(Bucket=S3_BUCKET, Key=filename, Body=json.dumps(msg.value))
        print(f"Uploaded: {filename} — {msg.value['name']}")