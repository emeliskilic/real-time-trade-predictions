run-dev:
	poetry run python src/main.py

build:
	docker build -t kafka-to-feature-store .

run:
	docker run \
	--network=redpanda_network \
	--env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
	kafka-to-feature-store

lint:
	ruff check --fix

format:
	ruff format .

lint-and-format:
	lint format