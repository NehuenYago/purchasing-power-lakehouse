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

# run spark processing/spark_jobs/process_donation.py file
run-process-donations:
	docker exec -it purchasing-power-lakehouse-spark-master-1 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/bitnami/spark/processing/spark_jobs/process_donations.py

# run spark processing/spark_jobs/process_currency.py file
run-process-currency:
	docker exec -it purchasing-power-lakehouse-spark-master-1 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/bitnami/spark/processing/spark_jobs/process_currency.py

# run spark processing/spark_jobs/calculate-actual-uds.py file
run-process-calculate:
	docker exec -it purchasing-power-lakehouse-spark-master-1 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/bitnami/spark/processing/spark_jobs/calculate_actual_usd.py

# run spark processing/spark_jobs/calculate-actual-uds.py file
run-process-totals:
	docker exec -it purchasing-power-lakehouse-spark-master-1 spark-submit /opt/bitnami/spark/processing/spark_jobs/calculate_campaign_totals.py

# show the output table created by spark
check-data:
	docker exec -it purchasing-power-lakehouse-spark-master-1 spark-submit /opt/bitnami/spark/processing/scripts/check_results.py

# Delete checkpoints and data
clean-checkpoints-data:
	sudo find processing/data/actual_donations_usd/ -mindepth 1 -delete & sudo find processing/checkpoints/ -mindepth 1 -delete
