build:
	docker build -t trade-to-ohlc .

run:
	docker run \
	--network=redpanda_network \
	--env KAFKA_BROKER_ADDRESS=redpanda-0:9092 \
	trade-to-ohlc

lint:
	ruff check --fix

format:
	ruff format .