import os
from pathlib import Path

import tomli
from pyspark.conf import SparkConf


class Configuration:
    def __init__(self):
        self.conf = self.load_configuration_from_file()
        self.spark_conf = self.build_spark_conf()

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "instance"):
            cls.instance = super(Configuration, cls).__new__(cls)
        return cls.instance

    def load_configuration_from_file(self):
        config_file_path = Path(os.getenv("CONFIG_FILE", "configuration.toml"))
        config_file = config_file_path if config_file_path.exists() else "configuration.toml"
        configuration = tomli.loads(Path(__file__).parent.joinpath(config_file).read_text())
        return configuration

    def build_spark_conf(self):
        conf = SparkConf()
        conf.setAppName(self.conf["app"]["app_name"])
        conf.set("spark.app.id", self.conf["app"]["app_id"])
        conf.set("spark.executor.extraJavaOptions", self.conf["spark"]["extra_java_options"])
        return conf
