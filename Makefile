SHELL=/bin/bash

compose_up=docker-compose --env-file .env up -d --build --remove-orphans
compose_down=docker-compose rm -fsv
services?=jupyter
pypi_host?=localhost

start:
	${compose_up} ${services}
stop:
	${compose_down} ${services}
stream:
	set -o allexport; \
	source .env; \
	set +o allexport; \
	python3 scripts/python/stream_to_kafka.py;
