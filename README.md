# Enterprise Master Data Platform

A configuration-driven MDM platform: ingests heterogeneous sources, standardizes to a canonical model, validates quality, resolves duplicate identities, and publishes trusted Golden Records, with end-to-end lineage and stage-by-stage reconciliation.

Built on **PySpark + Delta Lake, Medallion architecture, metadata-driven pipelines**.

## Quickstart

```bash
pip install pyspark==3.5.3 delta-spark==3.2.1 rapidfuzz pyyaml
python run_pipeline.py                     # local demo (Parquet storage)
TABLE_FORMAT=delta python run_pipeline.py  # on Databricks / any env with Delta jars
```

Requires Python 3.10+, Java 11+. The demo generates its own synthetic data, runs all stages, and prints the narrative: raw input -> standardized output -> quarantined records with reasons -> match decisions -> golden records -> reconciliation report -> a full lineage trace of one golden record.

## Repository layout

```
config/                     the control plane, adding sources/rules = editing YAML
  sources.yaml              source registry: format, field_map, trust_rank, load type
  standardization.yaml      ordered transforms per canonical attribute
  dq_rules.yaml             DQ rule engine rules (severity: ERROR quarantines, WARN annotates)
  matching.yaml             blocking keys, deterministic keys, fuzzy weights, thresholds
  survivorship.yaml         attribute-level survivorship strategies
src/
  common.py                 Spark session, table IO (Delta/Parquet switch), audit log
  data_generator.py         3 synthetic source systems with planted, traceable issues
  ingestion.py              Bronze: metadata-driven ingest, raw payload, watermarks
  standardization.py        Silver: transform registry + per-record transform log
  data_quality.py           DQ rule engine, quarantine, dq_results
  identity_resolution.py    blocking -> deterministic -> fuzzy scoring -> FP guards -> clustering
  golden_record.py          attribute-level survivorship, golden records, crosswalk
  reconciliation.py         stage reconciliation report + trace_golden_record()
run_pipeline.py             end-to-end demo
docs/
  design_document.md        architecture, decisions, trade-offs, NFRs
  data_model.md             logical model + table specs
```

## What the synthetic data proves

Every issue is planted deliberately, so the pipeline's behaviour is checkable against known ground truth:

| Planted issue | Expected platform behaviour | Verified in demo |
|---|---|---|
| Same person in CRM/WEB/LEGACY with format drift | standardization converges them; deterministic match merges | Rahul, Amit: 3 sources -> 1 golden record |
| Nickname + changed phone (Vikram -> "vicky singh") | email deterministic key still catches it; survivorship blends: CRM name + newer WEB phone | golden = "Vikram Singh", phone 9111222333 |
| Changed email AND phone (Divya on WEB) | no deterministic key survives; fuzzy score 0.75 -> REVIEW queue, not auto-merged | match_pairs: 0.75 probabilistic REVIEW |
| Two DIFFERENT people named Rohit Sharma, same city | candidate pair via name block, but conflicting DOB -> hard-rejected, never merged | match_pairs: REJECTED_FP_GUARD; two separate golden records |
| Invalid email `sneha[at]gmail` | standardization nulls it; DQ passes record via phone | golden Sneha, email NULL |
| Future date of birth | DQ006 ERROR -> quarantined | CRM C006 quarantined |
| Missing name / no contact identifiers | DQ001 / DQ002 ERROR -> quarantined | WEB U9998, U9997 quarantined |
| Duplicate primary key in one source | DQ007 ERROR -> quarantined | CRM C007 quarantined |
| Stale legacy timestamps | timeliness WARN, record proceeds annotated | LEGACY rows VALID_WITH_WARNINGS |
| Conflicting addresses (Rahul moved) | recency survivorship picks the newer address | survivorship_decisions: WEB wins address |

Reconciliation closes the loop: 24 ingested = 19 valid + 5 quarantined; 19 valid -> 12 golden records (7-record dedup consolidation, Divya held in review, the two Rohit Sharmas correctly kept apart); zero unexplained exceptions.

## Extending the platform (no code changes)

- **New source**: add an entry to `sources.yaml` with its `field_map` and `trust_rank`.
- **New DQ rule**: add YAML to `dq_rules.yaml` (rule types: not_null, regex, domain, expression, unique).
- **Different matching policy**: edit weights/thresholds/blocking in `matching.yaml`.
- **Different survivorship policy**: reorder strategies per attribute in `survivorship.yaml`, re-run gold.

See `docs/design_document.md` for architecture rationale, scaling to tens of millions of records, streaming/CDC support, observability and security.
