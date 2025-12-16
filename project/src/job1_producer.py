import json
import requests
from kafka import KafkaProducer
from datetime import datetime, timedelta


API_KEY = "vqh4IfE1Zy3GOQ2tN0GuNNIdAreJkBVK8NB11KrY"

def fetch_latest_data(region="PJM", hours=24):
    
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(hours=hours)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    url = (
        f"https://api.eia.gov/v2/electricity/rto/region-data/data/"
        f"?api_key={API_KEY}"
        f"&frequency=hourly"
        f"&data[0]=value"
        f"&facets[respondent][]={region}"
        f"&start={start_str}"
        f"&end={end_str}"
        f"&length=1000"
    )

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data.get("response", {}).get("data", [])

def send_to_kafka(data, topic="raw_events", kafka_server="localhost:9092"):
    
    if not data:
        print("No data to send")
        return

    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for record in data:
        producer.send(topic, record)
    producer.flush()
    producer.close()
    print(f"Sent {len(data)} records to Kafka topic '{topic}'")

def fetch_and_send(region="PJM", hours=24):

    records = fetch_latest_data(region, hours)
    send_to_kafka(records)
