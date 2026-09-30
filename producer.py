import json
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer
from config import *

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3
)

def fetch_trending_repos_for_day(target_date=None):
    if target_date is None:
        target_date = datetime.utcnow().date()

    start = datetime.combine(target_date, datetime.min.time())
    end = start + timedelta(days=1)

    # Use created:YYYY-MM-DD..YYYY-MM-DD for repos created that day
    query = f"pushed:{start.strftime('%Y-%m-%d')}..{end.strftime('%Y-%m-%d')}"

    params = {
        "q": query,
        "sort": "stars",
        "order": "desc",
        "per_page": 10
    }

    print(f"[{datetime.utcnow()}] Fetching repos for {target_date}...")
    response = requests.get(
        "https://api.github.com/search/repositories",
        params=params,
        timeout=15
    )
    response.raise_for_status()

    items = response.json().get("items", [])

    for idx, repo in enumerate(items, 1):
        message = {
            "rank": idx,
            "name": repo["name"],
            "owner": repo["owner"]["login"],
            "stars": repo["stargazers_count"],
            "language": repo.get("language"),
            "description": repo.get("description"),
            "url": repo["html_url"],
            "created_at": repo["created_at"],
            "collected_at": datetime.utcnow().isoformat() + "Z"
        }

        producer.send(KAFKA_TOPIC, value=message)
        print(f"  ✅ Sent: {idx}. {message['name']} ({message['stars']}⭐)")

    producer.flush()
    print(f"[{datetime.utcnow()}] Done: {len(items)} repos sent\n")


if __name__ == "__main__":
    fetch_trending_repos_for_day()