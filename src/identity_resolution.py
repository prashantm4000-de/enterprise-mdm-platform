"""Identity resolution engine.

Pipeline: blocking -> candidate pairs -> deterministic pass -> probabilistic
fuzzy scoring -> false-positive guards -> threshold decision -> clustering.

Blocking: records are compared only within shared block keys (config-defined
SQL expressions), turning O(n^2) comparison into O(n * block_size). At tens of
millions of records the same pattern holds; clustering moves from the driver
union-find used here to GraphFrames connected components.

Scoring: weighted fuzzy similarity (rapidfuzz) over configured attributes,
executed as a pandas-style UDF over candidate pairs. Deterministic key hits
score 1.0 outright.

Decisions land in gold.match_pairs with method + score (full explainability);
cluster assignments land in gold.entity_clusters.
"""
from pyspark.sql import SparkSession, functions as F, types as T
from src.common import load_yaml, write_table, read_table, audit_event
from rapidfuzz import fuzz

ATTRS = ["full_name", "email", "phone", "address_line", "city", "state", "country", "date_of_birth"]


def _score_udf(features):
    weights = [(f["attribute"], f["method"], float(f["weight"])) for f in features]

    @F.udf(T.DoubleType())
    def score(left, right):
        total = 0.0
        for attr, method, w in weights:
            a, b = left[attr], right[attr]
            if a is None or b is None:
                continue
            if method == "exact":
                total += w * (1.0 if a == b else 0.0)
            elif method == "token_sort_ratio":
                total += w * fuzz.token_sort_ratio(a, b) / 100.0
            elif method == "token_set_ratio":
                total += w * fuzz.token_set_ratio(a, b) / 100.0
        return round(total, 4)

    return score


def run(spark: SparkSession, batch_id: str) -> None:
    cfg = load_yaml("matching.yaml")
    df = read_table(spark, "silver", "customer_valid").select("record_uid", "source_id", "trust_rank",
                                                              "source_updated_at", *ATTRS)

    # ---- blocking ----
    block_exprs = [F.expr(k["expr"]) for k in cfg["blocking"]["keys"]]
    blocked = df.withColumn("block_key", F.explode(F.array(*block_exprs))).filter(F.col("block_key").isNotNull())

    l = blocked.alias("l")
    r = blocked.alias("r")
    pairs = (l.join(r, F.col("l.block_key") == F.col("r.block_key"))
              .filter(F.col("l.record_uid") < F.col("r.record_uid"))
              .select(F.struct(*[F.col(f"l.{c}") for c in ["record_uid"] + ATTRS]).alias("left"),
                      F.struct(*[F.col(f"r.{c}") for c in ["record_uid"] + ATTRS]).alias("right"))
              .dropDuplicates(["left", "right"]))

    # ---- deterministic pass ----
    det_conds = []
    for keyset in cfg["deterministic"]["keys"]:
        cond = F.lit(True)
        for k in keyset:
            cond = cond & F.col(f"left.{k}").isNotNull() & (F.col(f"left.{k}") == F.col(f"right.{k}"))
        det_conds.append(cond)
    is_det = det_conds[0]
    for c in det_conds[1:]:
        is_det = is_det | c

    # ---- probabilistic scoring ----
    score = _score_udf(cfg["probabilistic"]["features"])
    pairs = pairs.withColumn("fuzzy_score", score(F.col("left"), F.col("right")))
    pairs = pairs.withColumn("is_deterministic", is_det)
    pairs = pairs.withColumn("score", F.when(F.col("is_deterministic"), F.lit(1.0)).otherwise(F.col("fuzzy_score")))

    # ---- false-positive guards (hard disqualifiers) ----
    dob_conflict = (F.col("left.date_of_birth").isNotNull() & F.col("right.date_of_birth").isNotNull()
                    & (F.col("left.date_of_birth") != F.col("right.date_of_birth")))
    country_conflict = (F.col("left.country").isNotNull() & F.col("right.country").isNotNull()
                        & (F.col("left.country") != F.col("right.country")))
    pairs = pairs.withColumn("fp_rejected", (dob_conflict | country_conflict) & ~F.col("is_deterministic"))

    auto, review = cfg["thresholds"]["auto_match"], cfg["thresholds"]["review"]
    pairs = pairs.withColumn(
        "decision",
        F.when(F.col("fp_rejected"), "REJECTED_FP_GUARD")
         .when(F.col("score") >= auto, "MATCH")
         .when(F.col("score") >= review, "REVIEW")
         .otherwise("NO_MATCH"),
    )

    match_pairs = pairs.select(
        F.col("left.record_uid").alias("record_uid_1"),
        F.col("right.record_uid").alias("record_uid_2"),
        "score",
        F.when(F.col("is_deterministic"), "deterministic").otherwise("probabilistic").alias("method"),
        "decision",
        F.lit(batch_id).alias("batch_id"),
    )
    write_table(match_pairs, "gold", "match_pairs")

    # ---- clustering (union-find; GraphFrames connected components at scale) ----
    edges = [(row.record_uid_1, row.record_uid_2)
             for row in match_pairs.filter(F.col("decision") == "MATCH").collect()]
    all_uids = [row.record_uid for row in df.select("record_uid").distinct().collect()]

    parent = {u: u for u in all_uids}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in edges:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    clusters = [(u, "E-" + find(u)[:12]) for u in all_uids]
    cluster_df = spark.createDataFrame(clusters, ["record_uid", "cluster_id"])
    write_table(cluster_df, "gold", "entity_clusters")

    audit_event("identity_resolution", "ALL", batch_id, {
        "candidate_pairs": pairs.count(),
        "auto_matches": match_pairs.filter("decision = 'MATCH'").count(),
        "review_queue": match_pairs.filter("decision = 'REVIEW'").count(),
        "fp_rejected": match_pairs.filter("decision = 'REJECTED_FP_GUARD'").count(),
        "entities": cluster_df.select("cluster_id").distinct().count(),
    })
