PRODUCERS_DIR = ingestion/producers

.PHONY: up down clean run-dollar run-donations

# Docker compose
up:
	docker compose up -d

down:
	docker compose down

clean:
	docker compose down -v

# run producers
run-dollar:
	uv run $(PRODUCERS_DIR)/dollar_api_producer.py

run-donations:
	uv run $(PRODUCERS_DIR)/donation_simulator.py

# observability with kafkacat
watch-currency:
	kafkacat -b localhost:9092 -t raw_currency -C

watch-donations:
	kafkacat -b localhost:9092 -t raw_donations -C

list-topics:
	kafkacat -b localhost:9092 -L

