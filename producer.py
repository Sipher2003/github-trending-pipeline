from kafka import KafkaProducer
import requests, json, time, schedule
from config import *

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_trending():
    params = {'q': 'language:python', 'sort': 'stars', 'per_page': 10}
    response = requests.get(GITHUB_API_URL, params=params)
    for repo in response.json()['items']:
        message = {
            'name': repo['name'],
            'stars': repo['stargazers_count'],
            'url': repo['html_url']
        }
        producer.send(KAFKA_TOPIC, value=message)
        print(f"Sent: {message['name']} ({message['stars']} stars)")
    producer.flush()

if __name__ == "__main__":
    fetch_trending()