My take on Didomi challenge (Data engineering): https://github.com/didomi/challenges

<br/>

# Documentation

To ensure reproductibility given some dependencies that relies a lot on environment variables (Spark, Hadoop, Java), varying from each operating system, the application is delivered as a **Docker image** to be built and run. All sources are included.

<br/>

## The challenge

Due to time constraints to build a deployable cluster environment on AWS and dependencies that would have to be provided by reviewers to run the application on a cluster (such as credentials) I decided to focus primarly on delivering a Spark application that runs in local mode.

The code is fundamentally runnable as-is in cluster mode but I/O functions such as loading or writing data to S3 still needs to be fully implemented.

Rather than loading directly the challenge input data with `spark.read.json()`, I also made the choice to use **Hive** in local mode to ingest the data and make use of the provided `datehour` partitionning.

<br/>

## Code structure

Code is structured as a [monorepository](https://en.wikipedia.org/wiki/Monorepo) that can be expanded with new Spark jobs, schemas and data sources while reusing most of the same codebase.

```
didomi-data-challenge
├── LICENSE.txt
├── Makefile             # used to build and run Docker images
├── README.md            # this file
├── didomi_spark         # main package
│   ├── __init__.py
│   ├── app.py           # application entrypoint (main)
│   ├── core             # core components (spark session, logs, configuration)
│   ├── data             # provided input data for local mode
│   ├── jobs             # Spark jobs
│   ├── lib              # additionnal libs / jars needed
│   ├── repositories     # classes and functions used to interact with data sources
│   ├── schemas          # dataframes schemas
│   └── tests            # tests for the application (run spark jobs)
├── docker
│   ├── Dockerfile       # Dockerfile used to run the application
│   └── Dockerfile.test  # Dockerfile used to test the application
├── docs                 # Documentation sources for Sphinx 
│   ├── Makefile        
│   ├── conf.py
│   └── index.rst
├── pyproject.toml
├── requirements-dev.in  # dev requirements
├── requirements-dev.txt # compiled dev requirements with pip-compile
├── requirements.in      # requirements
├── requirements.txt     # compiled requirements with pip-compile
└── setup.py
```

<br/>

## Requirements

* Internet access (for Docker images dependencies and package dependencies)
* Docker
* make (if not present, `apt install make` on Linux or `brew install make` on Mac)
* the current sources (didomi-data-challenge)

<br/>

A Makefile is provided and targets provides convenient shortcuts for each task.

<br/>

## Build the Docker image


To build the image, at the root directory run the command:

`make build`

<br/>

## Run the Docker image

`make run`

This will call the `app.py` entrypoint with keyword arguments `app=events` and `mode=local`.

When the Spark application has completed, a directory `output` will be created in the local current directory with the job output written as compressed Parquet files partionned by `datehour`.

```
output/
├── datehour=2021-01-23-10
│   ├── part-00000-3c53f32c-c942-4102-958a-14fd08d00ad3.c000.snappy.parquet
│   └── part-00000-968f3f98-0ea6-4fee-b6ab-65b1c79dae50.c000.snappy.parquet
├── datehour=2021-01-23-11
│   ├── part-00000-3c53f32c-c942-4102-958a-14fd08d00ad3.c000.snappy.parquet
│   └── part-00000-968f3f98-0ea6-4fee-b6ab-65b1c79dae50.c000.snappy.parquet
└── _SUCCESS
```

<br/>

## Run the tests

Similarly, tests can be run with:

`make build-test` and then `make test`

or more succintly `make build-test test`

Tests output are then printed on the console as well as code coverage. Since those are not unit tests but functionnal tests running Spark jobs, this can take some time.

<br/>

## Setting up a development environment

* prerequisites: OpenJDK 11
* create a dedicated Python virtual environment, ie. with [conda](https://docs.conda.io/en/latest/miniconda.html): `conda create -n didomi-spark python=3.9 pip`
* install dev dependencies with `pip install -r requirements-dev.txt`

<br/>

## Updating dependencies

The requirements files `requirements.txt` and `requirements-dev.txt` are generated with `pip-compile` from [pip-tools](https://github.com/jazzband/pip-tools). 

* update `requirements.in` and `requirements-dev.in` as needed
* run `pip-compile requirements.in` or `pip-compile requirements-dev.in` to update the compiled requirements.

<br/>

## Building the documentation

API Documentation from docstrings can be built with `make build-docs` and will be available at didomi-data-challenge/docs/_build/html/index.html