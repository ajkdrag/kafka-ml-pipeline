SHELL=/bin/bash

compose_up=docker-compose --env-file .env up -d --build --remove-orphans
compose_down=docker-compose rm -fsv
services?=spark-jupyter
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
build:
	rm -rf ./dist ./spark/apps/dist && mkdir ./dist
	cp ./src/main.py ./dist
	cp ./src/configs/* ./dist
	cp scripts/bash/submit_jobs.sh ./dist
	cd src && \
	zip -r ../dist/jobs.zip jobs && \
	zip -r ../dist/schemas.zip schemas && \
	zip -r ../dist/utils.zip utils
	cd ..
	mv dist spark/apps