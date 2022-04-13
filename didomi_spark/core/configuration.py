import os
from pathlib import Path
from typing import Dict

import tomli
from pyspark.conf import SparkConf


class Configuration:
    """Centralize configuration for the application. This is a singleton class.

    Returns:
        Configuration: singleton instance of :obj:`~didomi_spark.core.configuration.Configuration`
    """

    def __init__(self):
        self.conf: Dict = self.load_configuration_from_file()
        self.spark_conf: SparkConf = self.build_spark_conf()
        self.events_path_local: str = self.conf["spark"]["events_path_local"]
        self.events_path_s3: str = self.conf["spark"]["events_path_s3"]
        self.output_path_local: str = self.conf["spark"]["output_path_local"]
        self.output_path_s3: str = self.conf["spark"]["output_path_s3"]

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "instance"):
            cls.instance = super(Configuration, cls).__new__(cls)
        return cls.instance

    def load_configuration_from_file(self) -> Dict:
        """Loads configuration from file in TOML format with path declared
        in environment variable CONFIG_FILE or use the bundled configuration file as default.

        Returns:
            Dict: parsed TOML configuration
        """
        config_file_path = Path(os.getenv("CONFIG_FILE", "configuration.toml"))
        config_file = config_file_path if config_file_path.exists() else "configuration.toml"
        configuration = tomli.loads(Path(__file__).parent.joinpath(config_file).read_text())
        return configuration

    def build_spark_conf(self) -> SparkConf:
        """Apply settings from configuration file to SparkConf.

        Returns:
            SparkConf: SparkConf instance to use in SparkSession.builder
        """
        conf = SparkConf()
        conf.setAppName(self.conf["app"]["app_name"])
        conf.set("spark.app.id", self.conf["app"]["app_id"])
        conf.set("spark.executor.extraJavaOptions", self.conf["spark"]["extra_java_options"])
        return conf
