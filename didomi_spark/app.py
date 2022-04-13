import sys

from didomi_spark.core.logs import logger
from didomi_spark.core.session import build_spark_session
from didomi_spark.jobs.events import EventJob

applications = {"events": EventJob}


def run_application(**kwargs):
    """Entrypoint to run a specified application based on those availables in the "applications" variable."""
    app = kwargs.get("app", None)
    mode = kwargs.get("mode", "cluster")
    if app in applications:
        cluster_mode = True if mode == "cluster" else False
        spark_session = build_spark_session(cluster_mode)
        job = applications[app](spark_session)
        job.run(cluster_mode)
    else:
        logger.error(f"Application {app} unkown.")


if __name__ == "__main__":
    kwargs = dict(x.split("=", 1) for x in sys.argv[1:])
    run_application(**kwargs)
