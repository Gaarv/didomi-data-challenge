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
	docker run --name didomi-data-challenge-test $(APP):test pytest --cov -v -s

build-docs:
	sphinx-apidoc -f -M -o docs/source/ didomi_spark/ "**/tests/"
	cp README.md docs/source/
	$(MAKE) -C docs html

build:
	docker build -f docker/Dockerfile -t $(APP) .
	docker tag $(APP) $(APP):latest

run:
	docker run --name didomi-data-challenge $(APP):latest python /app/didomi_spark/app.py app=events mode=local
	docker cp didomi-data-challenge:/app/output .