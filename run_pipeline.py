"""End-to-end demo runner.

  python run_pipeline.py

Stages: generate synthetic data -> bronze ingest -> silver standardize ->
DQ validate -> identity resolution -> golden records -> reconciliation ->
sample lineage trace. Prints a demonstration narrative at each stage.
"""
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src import data_generator
from src.common import get_spark, read_table
from src import ingestion, standardization, data_quality, identity_resolution, golden_record, reconciliation
from pyspark.sql import functions as F


def header(title):
    print("\n" + "=" * 78 + f"\n  {title}\n" + "=" * 78)


def main():
    # fresh, reproducible demo: clear prior state (watermarks make reruns incremental)
    import shutil
    root = os.path.dirname(os.path.abspath(__file__))
    for d in ["warehouse", "output"]:
        shutil.rmtree(os.path.join(root, d), ignore_errors=True)
    os.makedirs(os.path.join(root, "output"), exist_ok=True)

    header("STAGE 0 — SYNTHETIC SOURCE DATA")
    data_generator.main()

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    header("STAGE 1 — BRONZE INGESTION (metadata-driven)")
    batch_id = ingestion.run(spark)

    header("STAGE 2 — SILVER STANDARDIZATION (config-driven)")
    standardization.run(spark, batch_id)
    print("\nSample raw -> standardized:")
    (read_table(spark, "silver", "customer_standardized")
     .select("source_id", "source_record_id", "full_name", "email", "phone", "city", "date_of_birth")
     .orderBy("full_name").show(30, truncate=False))

    header("STAGE 3 — DATA QUALITY VALIDATION")
    data_quality.run(spark, batch_id)
    print("\nQuarantined records and why:")
    (read_table(spark, "silver", "dq_results")
     .filter("severity = 'ERROR'")
     .join(read_table(spark, "silver", "customer_standardized").select("record_uid", "source_record_id").distinct(), "record_uid")
     .select("source_id", "source_record_id", "rule_id", "description").distinct().show(truncate=False))

    header("STAGE 4 — IDENTITY RESOLUTION")
    identity_resolution.run(spark, batch_id)
    print("\nMatch decisions (non NO_MATCH):")
    (read_table(spark, "gold", "match_pairs")
     .filter("decision <> 'NO_MATCH'")
     .select("record_uid_1", "record_uid_2", "score", "method", "decision")
     .orderBy(F.desc("score")).show(30, truncate=25))

    header("STAGE 5 — GOLDEN RECORDS (attribute-level survivorship)")
    golden_record.run(spark, batch_id)
    print("\nGolden customer records:")
    (read_table(spark, "gold", "golden_customer")
     .select("golden_id", "full_name", "email", "phone", "city", "state", "date_of_birth")
     .orderBy("full_name").show(truncate=False))

    header("STAGE 6 — RECONCILIATION")
    report = reconciliation.run(spark, batch_id)
    print(json.dumps(report, indent=2))

    header("STAGE 7 — LINEAGE: trace one Golden Record end-to-end")
    sample = (read_table(spark, "gold", "crosswalk")
              .groupBy("golden_id").count().filter("count >= 3")
              .orderBy(F.desc("count")).first())
    trace = reconciliation.trace_golden_record(spark, sample["golden_id"])
    print(json.dumps(trace, indent=2, default=str)[:4000])
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", "sample_lineage_trace.json"), "w") as f:
        json.dump(trace, f, indent=2, default=str)

    header("DONE — outputs in ./output, tables in ./warehouse")
    spark.stop()


if __name__ == "__main__":
    main()
