"""Shared utilities: Spark session, config loading, table IO, audit logging.

Storage format is a config switch. The platform is designed for Delta Lake
(ACID upserts, schema evolution, time travel). Set TABLE_FORMAT=delta when
running on Databricks or any environment with the Delta jars available;
the demo sandbox falls back to Parquet transparently.
"""
import os
import json
import uuid
import datetime
import yaml
from pyspark.sql import SparkSession, DataFrame

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE = os.path.join(ROOT, "warehouse")
TABLE_FORMAT = os.environ.get("TABLE_FORMAT", "parquet")  # "delta" on Databricks


def get_spark(app_name: str = "mdm-platform") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    if TABLE_FORMAT == "delta":
        builder = (
            builder.config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    return builder.getOrCreate()


def load_yaml(name: str) -> dict:
    with open(os.path.join(ROOT, "config", name)) as f:
        return yaml.safe_load(f)


def table_path(layer: str, name: str) -> str:
    return os.path.join(WAREHOUSE, layer, name)


def write_table(df: DataFrame, layer: str, name: str, mode: str = "overwrite") -> None:
    df.write.format(TABLE_FORMAT).mode(mode).save(table_path(layer, name))


def read_table(spark: SparkSession, layer: str, name: str) -> DataFrame:
    return spark.read.format(TABLE_FORMAT).load(table_path(layer, name))


def new_batch_id() -> str:
    return datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]


def audit_event(stage: str, source_id: str, batch_id: str, metrics: dict) -> None:
    """Append-only JSONL audit log. In production this lands in a Delta audit
    table + is emitted as structured logs for the observability stack."""
    os.makedirs(os.path.join(ROOT, "output"), exist_ok=True)
    event = {
        "event_ts": datetime.datetime.utcnow().isoformat() + "Z",
        "stage": stage,
        "source_id": source_id,
        "batch_id": batch_id,
        **metrics,
    }
    with open(os.path.join(ROOT, "output", "audit_log.jsonl"), "a") as f:
        f.write(json.dumps(event) + "\n")
    print(f"[AUDIT] {stage:<22} source={source_id:<8} {metrics}")
