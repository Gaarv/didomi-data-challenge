import sys

from pyspark.sql import SparkSession

from didomi_spark.core.configuration import Configuration
from didomi_spark.core.logs import logger
from didomi_spark.jobs.events import EventJob

applications = {"events": EventJob}


def run_application(**kwargs):
    app = kwargs.get("app", None)
    if app in applications:
        spark_session = SparkSession.builder.config(conf=Configuration().spark_conf).enableHiveSupport().getOrCreate()
        applications["app"](spark_session).run()
    else:
        logger.error(f"Application {app} unkown.")


if __name__ == "__main__":
    kwargs = dict(x.split("=", 1) for x in sys.argv[1:])
    run_application(**kwargs)
