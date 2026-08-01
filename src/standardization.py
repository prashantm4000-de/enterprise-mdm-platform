"""Silver layer: configuration-driven standardization.

A registry of named, reusable transforms. config/standardization.yaml declares
which transforms apply to which canonical attribute, in order. Adding a new
transform type = one registered function; applying it anywhere = YAML only.

Every applied transform is recorded per record into `transform_log` (attribute,
transform name, before -> after), which feeds the lineage view.
"""
from pyspark.sql import SparkSession, DataFrame, Column, functions as F
from src.common import load_yaml, write_table, read_table, audit_event

TITLES = ["Mr", "Mrs", "Ms", "Dr", "Shri", "Smt"]


# ---------------- transform registry: name -> fn(col, params) -> col ----------------
def _trim(c, p): return F.trim(c)
def _collapse_ws(c, p): return F.regexp_replace(c, r"\s+", " ")
def _title_case(c, p): return F.initcap(F.lower(c))
def _lower(c, p): return F.lower(c)
def _upper(c, p): return F.upper(c)
def _digits_only(c, p): return F.nullif(F.regexp_replace(c, r"[^0-9]", ""), F.lit(""))


def _strip_titles(c, p):
    pattern = r"^(?:" + "|".join(TITLES) + r")\.?\s+"
    return F.regexp_replace(c, pattern, "")


def _null_if_invalid_email(c, p):
    return F.when(c.rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"), c)


def _normalize_phone(c, p):
    # strip leading country code (91) or trunk prefix (0) down to 10-digit national number
    c1 = F.when(F.length(c) == 12, F.when(c.startswith("91"), c.substr(3, 10)).otherwise(c)) \
          .when(F.length(c) == 11, F.when(c.startswith("0"), c.substr(2, 10)).otherwise(c)) \
          .otherwise(c)
    return c1


def _expand_abbreviations(c, p):
    out = c
    for abbr, full in p["map"].items():
        out = F.regexp_replace(out, rf"\b{abbr}\b\.?", full)
    return out


def _reference_lookup(c, p):
    expr = None
    for k, v in p["map"].items():
        cond = F.upper(c) == k.upper()
        expr = F.when(cond, v) if expr is None else expr.when(cond, v)
    return expr.otherwise(c) if expr is not None else c


def _parse_date(c, p):
    out = None
    for fmt in p["formats"]:
        parsed = F.to_date(c, fmt)
        out = parsed if out is None else F.coalesce(out, parsed)
    return F.date_format(out, "yyyy-MM-dd")


REGISTRY = {
    "trim": _trim,
    "collapse_whitespace": _collapse_ws,
    "title_case": _title_case,
    "lower_case": _lower,
    "upper_case": _upper,
    "digits_only": _digits_only,
    "strip_titles": _strip_titles,
    "null_if_invalid_email": _null_if_invalid_email,
    "normalize_phone": _normalize_phone,
    "expand_abbreviations": _expand_abbreviations,
    "reference_lookup": _reference_lookup,
    "parse_date": _parse_date,
}


def run(spark: SparkSession, batch_id: str) -> None:
    cfg = load_yaml("standardization.yaml")["rules"]
    df = read_table(spark, "bronze", "customer_raw")

    log_entries = []  # array of structs per attribute: what changed
    for attr, steps in cfg.items():
        if attr not in df.columns:
            continue
        before = F.col(attr)
        col: Column = F.col(attr)
        for step in steps:
            fn = REGISTRY[step["transform"]]
            col = fn(col, step.get("params", {}))
        df = df.withColumn(f"__std_{attr}", col)
        log_entries.append(
            F.struct(
                F.lit(attr).alias("attribute"),
                before.cast("string").alias("raw_value"),
                F.col(f"__std_{attr}").cast("string").alias("standardized_value"),
                F.lit("|".join(s["transform"] for s in steps)).alias("transforms_applied"),
            )
        )

    # replace raw values with standardized, keep a per-record transform log
    df = df.withColumn("transform_log", F.array(*log_entries))
    for attr in cfg.keys():
        if f"__std_{attr}" in df.columns:
            df = df.drop(attr).withColumnRenamed(f"__std_{attr}", attr)

    write_table(df, "silver", "customer_standardized")
    audit_event("silver_standardize", "ALL", batch_id, {"rows_standardized": df.count()})
