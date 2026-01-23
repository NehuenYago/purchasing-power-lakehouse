import uuid
import json
import time
import random
from confluent_kafka import Producer

# set up client with external kafka listener inside docker
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# list of 'fake' campaigns
CAMPAIGNS = ["comedores-ba", "hospital-garrahan", "unicef-argentina", "medicos-sin-fronteras"]

def generate_donation():
    while True:
        payload = {
            "donation_id": str(uuid.uuid4()),
            "campaign_id": random.choice(CAMPAIGNS),
            "amount_ars": round(random.uniform(1000.0, 1000000.0), 2),
            "donor_type": random.choice(["individual", "corporate", "anonymous"]),
            "timestamp": time.time()
        }
        
        message_value = json.dumps(payload)
        
        # this message goes to raw_donations topic
        producer.produce('raw_donations', value=message_value)
        producer.flush()
        
        print(f"Donation sent: {payload['amount_ars']} ARS to {payload['campaign_id']}")
        
        # a donation events happens every 2 to 30 seconds
        time.sleep(random.randint(2, 30))

if __name__ == "__main__":
    generate_donation()
