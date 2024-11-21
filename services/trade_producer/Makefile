run-dev:
	poetry run python src/main.py

run-dev-historical:
	LIVE_OR_HISTORICAL=historical poetry run python src/main.py

build:
	docker build -t trade-producer .

run:
	docker run \
	--network=redpanda_network \
	--env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
	trade-producer

lint:
	ruff check --fix

format:
	ruff format .