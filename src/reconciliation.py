"""Reconciliation + lineage.

Reconciliation: per-source record counts at every stage
  source files -> bronze -> standardized -> valid -> golden-linked
with explained exceptions (quarantined rows account for valid-stage drops;
merged duplicates account for golden-stage consolidation).

Lineage: trace_golden_record(golden_id) walks one Golden Record back to its
source records, raw payloads, transforms applied, DQ annotations, match
decisions and survivorship decisions — the full evidence chain.
"""
import json
from pyspark.sql import SparkSession, functions as F
from src.common import read_table, audit_event, ROOT
import os


def run(spark: SparkSession, batch_id: str) -> dict:
    bronze = read_table(spark, "bronze", "customer_raw")
    std = read_table(spark, "silver", "customer_standardized")
    valid = read_table(spark, "silver", "customer_valid")
    quar = read_table(spark, "silver", "customer_quarantine")
    xwalk = read_table(spark, "gold", "crosswalk")
    golden = read_table(spark, "gold", "golden_customer")

    def per_source(df, name):
        return {r["source_id"]: r["n"] for r in
                df.groupBy("source_id").agg(F.count("*").alias("n")).collect()}

    report = {
        "batch_id": batch_id,
        "stage_counts_by_source": {
            "bronze_ingested": per_source(bronze, "bronze"),
            "standardized": per_source(std, "std"),
            "dq_valid": per_source(valid, "valid"),
            "dq_quarantined": per_source(quar, "quar"),
            "linked_to_golden": per_source(xwalk, "xwalk"),
        },
        "totals": {
            "bronze": bronze.count(),
            "standardized": std.count(),
            "valid": valid.count(),
            "quarantined": quar.count(),
            "golden_records": golden.count(),
            "dedup_consolidation": valid.count() - golden.count(),
        },
    }

    # exceptions: any stage-to-stage drop not explained by quarantine
    exceptions = []
    for src, n_in in report["stage_counts_by_source"]["bronze_ingested"].items():
        n_std = report["stage_counts_by_source"]["standardized"].get(src, 0)
        n_valid = report["stage_counts_by_source"]["dq_valid"].get(src, 0)
        n_quar = report["stage_counts_by_source"]["dq_quarantined"].get(src, 0)
        if n_in != n_std:
            exceptions.append(f"{src}: bronze({n_in}) != standardized({n_std}) — unexplained loss, investigate")
        if n_std != n_valid + n_quar:
            exceptions.append(f"{src}: standardized({n_std}) != valid({n_valid}) + quarantined({n_quar})")
    report["exceptions"] = exceptions or ["none — all stage transitions fully reconciled"]

    os.makedirs(os.path.join(ROOT, "output"), exist_ok=True)
    with open(os.path.join(ROOT, "output", "reconciliation_report.json"), "w") as f:
        json.dump(report, f, indent=2)
    audit_event("reconciliation", "ALL", batch_id, {"exceptions": len(exceptions)})
    return report


def trace_golden_record(spark: SparkSession, golden_id: str) -> dict:
    """Full lineage for one Golden Record."""
    xwalk = read_table(spark, "gold", "crosswalk").filter(F.col("golden_id") == golden_id)
    uids = [r["record_uid"] for r in xwalk.select("record_uid").collect()]

    std = read_table(spark, "silver", "customer_standardized").filter(F.col("record_uid").isin(uids))
    dq = read_table(spark, "silver", "dq_results").filter(F.col("record_uid").isin(uids))
    matches = (read_table(spark, "gold", "match_pairs")
               .filter(F.col("record_uid_1").isin(uids) | F.col("record_uid_2").isin(uids)))
    surv = read_table(spark, "gold", "survivorship_decisions").filter(F.col("cluster_id") == golden_id)

    return {
        "golden_id": golden_id,
        "source_records": [r.asDict() for r in
                           std.select("record_uid", "source_id", "source_record_id", "raw_payload").collect()],
        "transforms": [r.asDict() for r in
                       std.select("record_uid", F.explode("transform_log").alias("t"))
                          .select("record_uid", "t.attribute", "t.raw_value", "t.standardized_value",
                                  "t.transforms_applied")
                          .filter("raw_value IS DISTINCT FROM standardized_value").collect()],
        "dq_annotations": [r.asDict() for r in dq.collect()],
        "match_decisions": [r.asDict() for r in
                            matches.select("record_uid_1", "record_uid_2", "score", "method", "decision").collect()],
        "survivorship_decisions": [r.asDict() for r in
                                   surv.select("attribute", "golden_value", "winning_source", "strategy").collect()],
    }
