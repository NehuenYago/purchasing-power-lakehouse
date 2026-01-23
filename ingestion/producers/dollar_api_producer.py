import json
import time
import requests
from confluent_kafka import Producer

# set up this python script (client) with EXTERNAL Kafka listener inside Docker
config = {
    'bootstrap.servers': 'localhost:9092',
}

# Kafka response
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# initialize producer
producer = Producer(config)

def fetch_and_send():
    url = "https://criptoya.com/api/dolar"
    
    while True:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status() 
            prices = response.json()
            blue_price = prices['blue']['ask']
            mep_price = prices['mep']['al30']['24hs']['price']

            payload = {
                "timestamp": time.time(),
                "provider": "criptoya",
                "rates": {
                    "blue": blue_price,
                    "mep": mep_price
                }
            }

            # turn payload dict into JSON string
            message_value = json.dumps(payload)

            # produces message to "raw_currency" topic and delivery_report will be called
            producer.produce("raw_currency", value="message_value", on_delivery="delivery_report")
            
            # pushes message form internal buffer to Kafka
            producer.flush()

        except requests.exceptions.RequestException as e:
            print(f"Error: {e}. Retrying in 60 seconds...")
            time.sleep(60)
            continue

        # waits 5 minutes to fetch again
        time.sleep(300)

if __name__ == "__main__":
    print("Starting USD Producer...")
    fetch_and_send()
