# Full Command Reference — GitHub Trending Kafka Pipeline
### Every command used, organized by where you run it

Legend: 🖥️ = Local Windows (CMD/PowerShell) · 🌐 = EC2 server (via SSH) · 🐍 = Python venv terminal

---

## 1. Local AWS Setup 🖥️

```bash
# Configure AWS CLI credentials
aws configure

# Verify credentials work
aws s3 ls

# Confirm identity (useful for debugging permission issues)
aws sts get-caller-identity
```

---

## 2. Local Project Folder Setup 🖥️

```bash
mkdir github-trending-pipeline
cd github-trending-pipeline

python -m venv venv
venv\Scripts\activate

pip install kafka-python requests boto3 schedule pandas
pip freeze > requirements.txt
```

---

## 3. Connecting to EC2 🖥️

```bash
# Fix Windows key file permissions (run once per key file)
icacls kafka-project-key.pem /reset
icacls kafka-project-key.pem /grant:r "%username%":R
icacls kafka-project-key.pem /inheritance:r
icacls kafka-project-key.pem          # verify — should show only your username

# Connect via SSH (IP changes every stop/start — always get the current one)
ssh -i kafka-project-key.pem ec2-user@YOUR_CURRENT_IP
```

---

## 4. Inside EC2 — Initial Setup 🌐

```bash
# Update system
sudo dnf update -y

# Install Java (Amazon Linux 2023 uses dnf + Corretto, not amazon-linux-extras)
sudo dnf install java-17-amazon-corretto -y
java -version

# Download and install Kafka
wget https://archive.apache.org/dist/kafka/3.5.0/kafka_2.13-3.5.0.tgz
tar -xzf kafka_2.13-3.5.0.tgz
sudo mv kafka_2.13-3.5.0 /opt/kafka
ln -s /opt/kafka ~/kafka
ls ~/kafka       # verify: should show bin, config, libs, etc.
```

---

## 5. Getting the Instance's Public IP 🌐

```bash
# Simple version (fails on instances requiring IMDSv2)
curl http://169.254.169.254/latest/meta-data/public-ipv4

# IMDSv2 version (works when metadata requires a token)
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600") && curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4
```
*(In practice, you already know your IP from the SSH command you used to connect — no need to run this unless double-checking.)*

---

## 6. Configuring Kafka's Advertised Listener 🌐

```bash
# The CORRECT property name is advertised.listeners (with a "d")
echo 'advertised.listeners=PLAINTEXT://YOUR_CURRENT_IP:9092' >> ~/kafka/config/server.properties

# Verify it saved correctly (no leading # , correct spelling)
grep advertised ~/kafka/config/server.properties
```
⚠️ Type quote-containing commands manually rather than pasting — pasted quotes sometimes get auto-converted to "smart quotes," which breaks bash.

---

## 7. Starting Zookeeper — Terminal 1 🌐

```bash
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
# Look for: "binding to port" — leave this terminal running, don't close it
```

---

## 8. Starting the Kafka Broker — Terminal 2 🌐

```bash
export KAFKA_HEAP_OPTS="-Xmx256M -Xms256M"
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
# Look for: "[KafkaServer id=0] started"
# Confirm in the config dump: advertised.listeners = PLAINTEXT://YOUR_IP:9092 (not null)
```

---

## 9. Creating and Managing Topics — Terminal 3 🌐

```bash
cd ~/kafka

# Create the topic
bin/kafka-topics.sh --create --topic github-trending --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# List all topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Check how many messages are in a topic (useful for debugging "nothing showing up")
bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic github-trending

# Delete and recreate a topic (if needed)
bin/kafka-topics.sh --delete --topic github-trending --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic github-trending --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

## 10. Manual Testing — Console Producer/Consumer 🌐

```bash
cd ~/kafka

# Console producer (type messages manually, Ctrl+C to stop)
bin/kafka-console-producer.sh --topic github-trending --bootstrap-server localhost:9092

# Console consumer (reads all messages from the start)
bin/kafka-console-consumer.sh --topic github-trending --bootstrap-server localhost:9092 --from-beginning
```

---

## 11. Python Scripts 🐍

```bash
# Navigate and activate venv (every new terminal session)
cd D:\GitHub\github-trending-pipeline
venv\Scripts\activate

# Run the producer (fetches from GitHub, sends to Kafka)
python producer.py

# Run the consumer (reads from Kafka, uploads to S3) — runs forever, Ctrl+C to stop
python consumer.py
```

---

## 12. S3 Commands 🖥️

```bash
# Create a bucket (must be globally unique, all lowercase)
aws s3 mb s3://your-bucket-name

# List all your buckets
aws s3 ls

# List contents of a specific folder inside a bucket
aws s3 ls s3://your-bucket-name/raw-data/ --recursive

# View a file's contents directly in the terminal (no download needed)
aws s3 cp s3://your-bucket-name/raw-data/filename.json -

# Delete everything in a folder (careful — irreversible)
aws s3 rm s3://your-bucket-name/raw-data/ --recursive
```

---

## 13. AWS Glue Commands 🖥️

```bash
# Create a Glue database
aws glue create-database --database-input "Name=github_trending_db"

# Verify it exists
aws glue get-databases

# After running a crawler in the console, check the table it created
aws glue get-tables --database-name github_trending_db

# Check crawler status
aws glue get-crawler --name github-trending-crawler

# Start a crawler from the CLI (alternative to clicking "Run" in console)
aws glue start-crawler --name github-trending-crawler
```

---

## 14. EC2 Instance Management 🖥️

```bash
# List your instances
aws ec2 describe-instances

# Stop an instance (saves cost, keeps it for later)
aws ec2 stop-instances --instance-ids i-xxxxxxxxx

# Start it again
aws ec2 start-instances --instance-ids i-xxxxxxxxx

# Terminate permanently (irreversible — deletes the instance)
aws ec2 terminate-instances --instance-ids i-xxxxxxxxx
```

---

## 15. Athena (Run inside the Athena Console query editor, not CLI)

```sql
-- Basic check
SELECT * FROM raw_data LIMIT 10;

-- Top 5 by stars
SELECT name, owner, stars, language
FROM raw_data
ORDER BY stars DESC
LIMIT 5;

-- Average stars by language
SELECT language, COUNT(*) AS repo_count, AVG(stars) AS avg_stars
FROM raw_data
WHERE language IS NOT NULL
GROUP BY language
ORDER BY avg_stars DESC;

-- Basic stats
SELECT MIN(stars) AS min_stars, MAX(stars) AS max_stars, AVG(stars) AS avg_stars, COUNT(*) AS total_repos
FROM raw_data;
```

---

## 16. Windows File Permission Fixes 🖥️
*(Only needed once per `.pem` file, or if you get "Bad permissions" again)*

```bash
icacls kafka-project-key.pem /reset
icacls kafka-project-key.pem /grant:r "%username%":R
icacls kafka-project-key.pem /inheritance:r
```

---

## Quick "Getting Back Up After a Restart" Checklist

Every time you stop and restart the EC2 instance, run through this order:

1. Start the instance in the EC2 console → note the new public IP
2. SSH in: `ssh -i kafka-project-key.pem ec2-user@NEW_IP`
3. Terminal 1: start Zookeeper
4. Terminal 2: `echo` the new `advertised.listeners` line → start Kafka broker
5. Terminal 3: check/recreate the topic
6. Locally: update `KAFKA_BROKER` in `config.py` with the new IP
7. Run `producer.py`, then `consumer.py`, as needed

---

## Command Categories at a Glance

| Where | What runs there |
|---|---|
| 🖥️ Local CMD/PowerShell | `aws` CLI commands, `icacls`, `ssh`, `git` |
| 🌐 EC2 (via SSH) | `dnf`, `wget`, `tar`, all `bin/kafka-*.sh` and `bin/zookeeper-*.sh` scripts |
| 🐍 Python venv | `python producer.py`, `python consumer.py`, `pip install` |
| Athena Console (browser) | SQL queries only — no CLI needed for this project |
