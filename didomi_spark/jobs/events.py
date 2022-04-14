from enum import Enum, unique
from pathlib import Path
from typing import Callable, List

from didomi_spark.core.logs import logger
from didomi_spark.core.configuration import Configuration
from didomi_spark.repositories.events import EventRepository
from didomi_spark.schemas.events import EventSchemas
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from s3path import S3Path

event_schemas = EventSchemas()
Transformation = Callable[[DataFrame], DataFrame]


@unique
class EventType(Enum):
    pageviews = "pageview"
    consents_asked = "consent.asked"
    consents_given = "consent.given"


class EventJob:
    """Class representing a job that process :obj:`~didomi_spark.schemas.events.EventSchemas` data."""

    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session = spark_session
        self.repository = EventRepository(self.spark_session)
        self.schemas = EventSchemas()
        self.conf = Configuration()

    def load(self, cluster_mode: bool = True) -> DataFrame:
        """Loads data from :obj:`~didomi_spark.repositories.events.EventRepository`

        Args:
            cluster_mode (bool, optional): Whether to run in clutser mode or local mode. Defaults to True.

        Returns:
            DataFrame: DataFrame with schema :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event`
        """
        if cluster_mode:
            events_path = S3Path(self.conf.events_path_s3)
            raw_events = self.repository.fetch_raw_events_from_s3(events_path)
        else:
            zip_file = Path(self.conf.events_path_local)
            events_path = self.repository.extract_from_file(zip_file)
            self.repository.create_hive_table()
            self.repository.load_into_hive(events_path)
            raw_events = self.repository.fetch_raw_events_from_hive()
        return raw_events

    def transform(self, df: DataFrame) -> DataFrame:
        """Perform aggregations. This function isolate core logic from input and output side effects.

        Args:
            df (DataFrame): DataFrame with schema :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event`

        Returns:
            DataFrame: DataFrame with schema :attr:`~didomi_spark.schemas.events.EventSchemas.events_metrics`
        """
        metrics = aggregate_events_metrics(df)
        return metrics

    def save(self, df: DataFrame, cluster_mode: bool = True) -> None:
        """Loads output DataFrame into defined output location.

        Args:
            df (DataFrame): DataFrame with schema :attr:`~didomi_spark.schemas.events.EventSchemas.events_metrics`
            cluster_mode (bool, optional): Whether to run in clutser mode or local mode. Defaults to True.
        """
        if cluster_mode:
            output_path = S3Path(self.conf.output_path_s3).as_uri()
        else:
            output_path = Path(self.conf.output_path_local).absolute().as_posix()

        logger.info(f"Saving output...", output_path=output_path)
        df.write.mode("overwrite").partitionBy("datehour").parquet(output_path)

    def run(self, cluster_mode: bool = True) -> None:
        """Compose functions used for :obj:`~didomi_spark.jobs.events.EventJob`.

        Args:
            cluster_mode (bool, optional): Whether to run in clutser mode or local mode. Defaults to True.
        """
        events = self.load(cluster_mode)
        self.save(self.transform(events), cluster_mode)


def map_events(raw_events: DataFrame) -> DataFrame:
    """Map :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event` to :attr:`~didomi_spark.schemas.events.EventSchemas.event`.

    Args:
        raw_events (DataFrame): :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event`

    Returns:
        DataFrame: :attr:`~didomi_spark.schemas.events.EventSchemas.event`
    """
    event_cols = [F.col(f"{c}").alias(f"event_{c}") for c in ["id", "type"]]
    raw_cols = [F.col(c) for c in ["domain", "datehour"]]
    user_cols = [F.col(f"user.{c}").alias(f"user_{c}") for c in ["id", "country"]]
    transform_cols = [F.col("user_consent")]
    events = raw_events.dropna(how="any").transform(extract_user_consent).select(event_cols + raw_cols + user_cols + transform_cols)
    return events


def extract_user_consent(raw_events: DataFrame) -> DataFrame:
    """Extract if user consent was given or not from :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event`

    Args:
        raw_events (DataFrame): :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event`

    Returns:
        DataFrame: :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event` with an additional column "user_consent"
    """
    token_data = raw_events.withColumn("token_data", F.from_json("user.token", schema=event_schemas.token))
    user_consent = token_data.withColumn("user_consent", F.when(F.size("token_data.purposes.enabled") > 0, 1).otherwise(0))
    user_consent = user_consent.drop("token_data", "user.token")
    return user_consent


def deduplicate_by_event_id(raw_events: DataFrame) -> DataFrame:
    """Deduplicate :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event` based on column "id

    Args:
        raw_events (DataFrame): :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event`

    Returns:
        DataFrame: :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event`
    """
    window = Window.partitionBy(F.col("datehour"), F.col("id")).orderBy("id")
    deduplicated = raw_events.withColumn("event_id_row", F.row_number().over(window)).filter(F.col("event_id_row") == 1)
    deduplicated = deduplicated.drop("event_id_row")
    return deduplicated


def aggregate_events_metrics(raw_events: DataFrame) -> DataFrame:
    """Aggregate metrics from :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event`

    Args:
        raw_events (DataFrame): :attr:`~didomi_spark.schemas.events.EventSchemas.raw_event`

    Returns:
        DataFrame: :attr:`~didomi_spark.schemas.events.EventSchemas.events_metrics`
    """
    deduplicated_raw_events = deduplicate_by_event_id(raw_events)
    events = map_events(deduplicated_raw_events).cache()  # cache before successive transformations
    aggregated = aggregate_by_event_type(
        events,
        events_types=[
            EventType.pageviews,
            EventType.consents_asked,
            EventType.consents_given,
        ],
    )
    aggregated_with_consent = aggregate_by_event_type(
        events,
        events_types=[
            EventType.pageviews,
            EventType.consents_asked,
            EventType.consents_given,
        ],
        with_consent=True,
    )
    metrics = (
        aggregated.join(aggregated_with_consent, on=["datehour", "domain", "country"], how="outer")
        .join(aggregate_avg_pageviews_per_user(events), on=["datehour", "domain", "country"], how="outer")
        .na.fill(0)
    )
    return metrics


def aggregate_by_event_type(events: DataFrame, events_types: List[EventType], with_consent: bool = False) -> DataFrame:
    """Factory function to aggregate :attr:`~didomi_spark.schemas.events.EventSchemas.event`
    depending if user consent was positive or negative.

    Args:
        events (DataFrame): :attr:`~didomi_spark.schemas.events.EventSchemas.event`
        events_types (List[EventType]): list of EventType to use for pivot
        with_consent (bool, optional): filter by positive user_consent if True. Defaults to False.

    Returns:
        DataFrame: DataFrame with columns "datehour", "domain", "country" with events_types and consent as selected.
    """
    # Filter based on consent
    filtered = events.filter(F.col("user_consent") == 1) if with_consent else events

    aggregated = (
        filtered.groupBy(F.col("datehour"), F.col("domain"), F.col("user_country").alias("country"))
        .pivot("event_type", values=[e.value for e in events_types])  # by explicitly settings pivot values, we avoid a spark computation
        .agg(F.count(F.col("event_type")))
        .na.fill(0)
    )

    # Rename columns based on consent
    suffix_name = "_with_consent" if with_consent else ""
    for event_type in events_types:
        aggregated = aggregated.withColumnRenamed(f"{event_type.value}", f"{event_type.name}{suffix_name}")

    return aggregated


def aggregate_avg_pageviews_per_user(events: DataFrame) -> DataFrame:
    """Aggregate the average pageviews per user from :attr:`~didomi_spark.schemas.events.EventSchemas.event`

    Args:
        events (DataFrame): :attr:`~didomi_spark.schemas.events.EventSchemas.event`

    Returns:
        DataFrame: :attr:`~didomi_spark.schemas.events.EventSchemas.events_metrics` with columns "datehour", "domain", "country" and "avg_pageviews_per_user"
    """
    avg_pageviews_per_user = (
        events.select(F.col("datehour"), F.col("domain"), F.col("user_country").alias("country"), F.col("user_id"), F.col("event_type"))
        .filter(F.col("event_type") == EventType.pageviews.value)
        .groupBy(F.col("datehour"), F.col("domain"), F.col("country"), F.col("user_id"))
        .agg(F.count("event_type").alias("pageviews_per_user"))
        .groupBy(F.col("datehour"), F.col("domain"), F.col("country"))
        .agg(F.round(F.avg("pageviews_per_user"), 2).alias("avg_pageviews_per_user"))
    )
    return avg_pageviews_per_user
