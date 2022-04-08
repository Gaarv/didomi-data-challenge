APP?=didomi-data-challenge

.PHONY: all

clean:
	docker stop $(shell docker ps -aq) || true
	docker rm $(shell docker ps -aq) || true
	docker rmi $(shell docker images --filter "dangling=true" -q) -f || true

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