"""Data quality rule engine.

Each rule in config/dq_rules.yaml has a rule_type that maps to a registered
check builder. A check builder returns a boolean Column (True = pass).

Outputs:
  silver.customer_valid       - records with no ERROR failures (WARNs annotated)
  silver.customer_quarantine  - records failing at least one ERROR rule
  silver.dq_results           - one row per record x failed rule (feeds lineage,
                                reconciliation and steward dashboards)
"""
from pyspark.sql import SparkSession, functions as F, Window
from src.common import load_yaml, write_table, read_table, audit_event


def _check_not_null(df, rule):
    return F.col(rule["column"]).isNotNull()


def _check_regex(df, rule):
    ok = F.col(rule["column"]).rlike(rule["pattern"])
    if rule.get("allow_null"):
        ok = F.col(rule["column"]).isNull() | ok
    return ok


def _check_domain(df, rule):
    ok = F.col(rule["column"]).isin(rule["allowed_values"])
    if rule.get("allow_null"):
        ok = F.col(rule["column"]).isNull() | ok
    return ok


def _check_expression(df, rule):
    return F.expr(rule["expression"])


def _check_unique(df, rule):
    w = Window.partitionBy(*[F.col(c) for c in rule["columns"]])
    return F.count("*").over(w) == 1


CHECKS = {
    "not_null": _check_not_null,
    "regex": _check_regex,
    "domain": _check_domain,
    "expression": _check_expression,
    "unique": _check_unique,
}


def run(spark: SparkSession, batch_id: str) -> None:
    rules = load_yaml("dq_rules.yaml")["rules"]
    df = read_table(spark, "silver", "customer_standardized")

    fail_structs = []
    for rule in rules:
        ok = CHECKS[rule["rule_type"]](df, rule)
        fail_structs.append(
            F.when(~F.coalesce(ok, F.lit(False)),
                   F.struct(F.lit(rule["rule_id"]).alias("rule_id"),
                            F.lit(rule["severity"]).alias("severity"),
                            F.lit(rule["description"]).alias("description")))
        )

    df = df.withColumn("dq_failures", F.filter(F.array(*fail_structs), lambda x: x.isNotNull()))
    df = df.withColumn(
        "dq_status",
        F.when(F.exists("dq_failures", lambda x: x["severity"] == "ERROR"), "QUARANTINED")
         .when(F.size("dq_failures") > 0, "VALID_WITH_WARNINGS")
         .otherwise("VALID"),
    )

    dq_results = (df.select("record_uid", "source_id", "batch_id", F.explode("dq_failures").alias("f"))
                    .select("record_uid", "source_id", "batch_id",
                            "f.rule_id", "f.severity", "f.description"))
    write_table(dq_results, "silver", "dq_results")

    valid = df.filter(F.col("dq_status") != "QUARANTINED")
    quarantine = df.filter(F.col("dq_status") == "QUARANTINED")
    write_table(valid, "silver", "customer_valid")
    write_table(quarantine, "silver", "customer_quarantine")

    audit_event("dq_validation", "ALL", batch_id, {
        "rows_in": df.count(),
        "rows_valid": valid.count(),
        "rows_quarantined": quarantine.count(),
        "rule_failures": dq_results.count(),
    })
