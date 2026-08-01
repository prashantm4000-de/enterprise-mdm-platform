"""Golden Record management: attribute-level survivorship.

For each entity cluster and each canonical attribute, candidate values from
member records compete under an ordered strategy list (config/survivorship.yaml):
  source_precedence -> lowest trust_rank wins
  most_recent       -> latest source_updated_at wins
  non_null_longest  -> longest non-null value wins
  non_null_any      -> any non-null value

Every winning value records WHICH record and WHICH strategy supplied it
(gold.survivorship_decisions) — attribute-level lineage, not just record-level.

gold.golden_customer  - one trusted record per entity
gold.crosswalk        - golden_id <-> source record mapping (the MDM crosswalk)
"""
from pyspark.sql import SparkSession, functions as F, Window
from src.common import load_yaml, write_table, read_table, audit_event

ATTRS = ["full_name", "email", "phone", "address_line", "city", "state", "country", "date_of_birth"]


def _order_for(strategy: str):
    if strategy == "source_precedence":
        return [F.col("trust_rank").asc_nulls_last()]
    if strategy == "most_recent":
        return [F.col("source_updated_at").desc_nulls_last()]
    if strategy == "non_null_longest":
        return [F.length(F.col("val")).desc_nulls_last()]
    return [F.lit(1).asc()]  # non_null_any: arbitrary stable pick


def run(spark: SparkSession, batch_id: str) -> None:
    cfg = load_yaml("survivorship.yaml")
    members = (read_table(spark, "silver", "customer_valid")
               .join(read_table(spark, "gold", "entity_clusters"), "record_uid"))

    decisions = None
    for attr in ATTRS:
        strategies = cfg["attributes"].get(attr, cfg["default_strategy"])
        cand = members.select("cluster_id", "record_uid", "source_id", "trust_rank",
                              "source_updated_at", F.col(attr).alias("val"))
        cand = cand.filter(F.col("val").isNotNull())
        # ordered strategy list -> composite sort: primary strategy first, then tie-breakers
        order = []
        for s in strategies:
            order += _order_for(s)
        order += [F.col("record_uid").asc()]  # determinism
        w = Window.partitionBy("cluster_id").orderBy(*order)
        winner = (cand.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
                  .select("cluster_id",
                          F.lit(attr).alias("attribute"),
                          F.col("val").alias("golden_value"),
                          F.col("record_uid").alias("winning_record_uid"),
                          F.col("source_id").alias("winning_source"),
                          F.lit("+".join(strategies)).alias("strategy")))
        decisions = winner if decisions is None else decisions.unionByName(winner)

    write_table(decisions, "gold", "survivorship_decisions")

    golden = (decisions.groupBy("cluster_id")
              .pivot("attribute", ATTRS)
              .agg(F.first("golden_value"))
              .withColumnRenamed("cluster_id", "golden_id")
              .withColumn("mdm_created_ts", F.current_timestamp())
              .withColumn("batch_id", F.lit(batch_id)))
    write_table(golden, "gold", "golden_customer")

    crosswalk = members.select(F.col("cluster_id").alias("golden_id"),
                               "record_uid", "source_id", "source_record_id", "batch_id")
    write_table(crosswalk, "gold", "crosswalk")

    audit_event("golden_record", "ALL", batch_id, {
        "golden_records": golden.count(),
        "source_records_linked": crosswalk.count(),
    })
