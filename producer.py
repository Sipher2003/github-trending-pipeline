import json
import time
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

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

def fetch_page(query, page):
    params = {
        "q": query,
        "sort": "stars",
        "order": "desc",
        "per_page": 100,
        "page": page
    }
    response = requests.get(
        "https://api.github.com/search/repositories",
        params=params,
        headers=HEADERS,
        timeout=15
    )

    # Respect rate limits proactively
    remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
    if remaining < 2:
        reset_time = int(response.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait = max(reset_time - int(time.time()), 1)
        print(f"  Rate limit low. Sleeping {wait}s...")
        time.sleep(wait)

    response.raise_for_status()
    return response.json().get("items", [])

def fetch_day(target_date):
    """Fetch up to 1000 repos created on a single specific day."""
    start = target_date.strftime('%Y-%m-%d')
    end = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')
    query = f"created:{start}..{end}"

    sent_count = 0
    for page in range(1, 11):  # GitHub caps at 10 pages x 100 = 1000 per query
        items = fetch_page(query, page)
        if not items:
            break

        for idx, repo in enumerate(items, 1):
            message = {
                "rank": (page - 1) * 100 + idx,
                "name": repo["name"],
                "owner": repo["owner"]["login"],
                "stars": repo["stargazers_count"],
                "language": repo.get("language"),
                "description": repo.get("description"),
                "url": repo["html_url"],
                "created_at": repo["created_at"],
                "fetch_date": start,
                "collected_at": datetime.utcnow().isoformat() + "Z"
            }
            producer.send(KAFKA_TOPIC, value=message)
            sent_count += 1

        print(f"  {start} page {page}: sent {len(items)} repos (running total: {sent_count})")
        time.sleep(2)  # polite pacing between pages

    return sent_count

def fetch_trending_bulk(days_window=7):
    total_sent = 0
    today = datetime.utcnow().date()

    for i in range(days_window):
        target_date = today - timedelta(days=i)
        target_datetime = datetime.combine(target_date, datetime.min.time())
        print(f"[{datetime.utcnow()}] Fetching repos created on {target_date}...")
        total_sent += fetch_day(target_datetime)

    producer.flush()
    print(f"\n[{datetime.utcnow()}] DONE. Total repos sent to Kafka: {total_sent}")

if __name__ == "__main__":
    fetch_trending_bulk(days_window=7)