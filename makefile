APP?=didomi-data-challenge
DANGLING:=$(shell docker images --filter "dangling=true" -q)

.PHONY: all

clean:
	docker rmi ${DANGLING} -f

build-test:
	docker build -f docker/Dockerfile.test  -t $(APP) .
	docker tag $(APP) $(APP):test

test:
	docker run $(APP):test pytest -v -s

build:
	docker build -f docker/Dockerfile  -t $(APP) .
	docker tag $(APP) $(APP):latest

run:
	docker run $(APP):latest python main.py