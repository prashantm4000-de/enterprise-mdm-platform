"""Bronze layer: metadata-driven ingestion.

For every source registered in config/sources.yaml:
  - read with the declared format/options (PERMISSIVE mode; corrupt rows are
    captured, not dropped — dead-letter pattern)
  - preserve the full raw record as a JSON string (immutable evidence for lineage)
  - stamp ingestion metadata: source_id, batch_id, ingest_ts, record_uid
  - apply the field_map to project source columns onto canonical column names
    (pure renaming here — no value transformation until Silver)
  - enforce the incremental watermark if load_type = incremental
  - write an audit event with row counts

record_uid = sha2(source_id || source_record_id) — the stable key used by
DQ results, match pairs, lineage and reconciliation throughout the platform.
"""
from pyspark.sql import SparkSession, DataFrame, functions as F
from src.common import load_yaml, write_table, audit_event, new_batch_id, ROOT
import os
import json


def _read_source(spark: SparkSession, src: dict) -> DataFrame:
    path = os.path.join(ROOT, src["path"])
    fmt = src["format"]
    opts = src.get("options", {}) or {}
    if fmt == "csv":
        reader = (spark.read.option("header", "true").option("mode", "PERMISSIVE")
                  .option("columnNameOfCorruptRecord", "_corrupt_record"))
        for k, v in opts.items():
            reader = reader.option(k, v)
        return reader.csv(path)
    if fmt == "json":
        return spark.read.option("mode", "PERMISSIVE").json(path)
    raise ValueError(f"Unsupported format: {fmt}")


def ingest_source(spark: SparkSession, src: dict, batch_id: str, watermark: str | None = None) -> DataFrame:
    df = _read_source(spark, src)
    raw_cols = df.columns

    # Preserve the untouched source record for lineage / replay
    df = df.withColumn("raw_payload", F.to_json(F.struct(*[F.col(c) for c in raw_cols])))

    # Project to canonical names (rename only)
    for src_col, canon in src["field_map"].items():
        if src_col in raw_cols:
            df = df.withColumnRenamed(src_col, canon)

    # Ingestion metadata
    df = (
        df.withColumn("source_id", F.lit(src["source_id"]))
          .withColumn("batch_id", F.lit(batch_id))
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn("record_uid", F.sha2(F.concat_ws("||", F.lit(src["source_id"]), F.col("source_record_id")), 256))
          .withColumn("trust_rank", F.lit(src.get("trust_rank", 99)))
    )

    # Incremental loading: only records newer than the stored watermark
    if src.get("load_type") == "incremental" and watermark:
        df = df.filter(F.col("source_updated_at") > F.lit(watermark))

    keep = ["record_uid", "source_id", "source_record_id", "batch_id", "ingest_ts",
            "trust_rank", "raw_payload", "source_updated_at",
            "full_name", "email", "phone", "address_line", "city", "state", "country", "date_of_birth"]
    for c in keep:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df.select(*keep)


def run(spark: SparkSession) -> str:
    cfg = load_yaml("sources.yaml")
    batch_id = new_batch_id()
    watermarks = _load_watermarks()
    frames = []
    for src in cfg["sources"]:
        df = ingest_source(spark, src, batch_id, watermarks.get(src["source_id"]))
        n = df.count()
        audit_event("bronze_ingest", src["source_id"], batch_id, {"rows_ingested": n})
        frames.append(df)
        # advance watermark
        if src.get("load_type") == "incremental":
            mx = df.agg(F.max("source_updated_at")).first()[0]
            if mx:
                watermarks[src["source_id"]] = str(mx)
    bronze = frames[0]
    for f in frames[1:]:
        bronze = bronze.unionByName(f)
    write_table(bronze, "bronze", "customer_raw")
    _save_watermarks(watermarks)
    return batch_id


def _wm_path():
    return os.path.join(ROOT, "output", "watermarks.json")


def _load_watermarks() -> dict:
    try:
        with open(_wm_path()) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def _save_watermarks(wm: dict) -> None:
    os.makedirs(os.path.dirname(_wm_path()), exist_ok=True)
    with open(_wm_path(), "w") as f:
        json.dump(wm, f, indent=2)
