"""Reusable, configuration-driven Spark workload benchmarking helpers.

This is an educational tuning toolkit: it compares a fixed workload using a
small set of candidates. It does not automatically rewrite arbitrary Spark jobs.
"""
from __future__ import annotations

import json
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any, Dict, List, Optional

try:
    from pyspark import StorageLevel
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
except ImportError:  # lets config validation run without PySpark installed
    StorageLevel = None
    DataFrame = SparkSession = Any  # type: ignore[misc,assignment]
    F = None

SUPPORTED_JOIN_TYPES = {"inner", "left", "right", "full", "left_semi", "left_anti"}
FILTER_OPERATORS = {"eq", "ne", "gt", "ge", "lt", "le", "isin", "is_null", "not_null"}
CANDIDATES = {"baseline", "broadcast", "repartition", "configured"}


def load_config(path: str) -> Dict[str, Any]:
    """Read JSON config and validate it before use."""
    with open(path, "r", encoding="utf-8") as handle:
        config = json.load(handle)
    validate_config(config)
    return config


def validate_config(config: Dict[str, Any]) -> None:
    for section in ("demo", "input", "join", "aggregation", "optimization", "benchmark", "output"):
        if section not in config:
            raise ValueError(f"Missing configuration section: {section}")
    join = config["join"]
    if not isinstance(join.get("key"), str) or not join["key"].strip():
        raise ValueError("join.key must be a non-empty column name")
    if join.get("type", "inner") not in SUPPORTED_JOIN_TYPES:
        raise ValueError(f"Unsupported join type: {join.get('type')}")
    agg = config["aggregation"]
    for name in ("group_by", "sum_columns"):
        if not isinstance(agg.get(name, []), list):
            raise ValueError(f"aggregation.{name} must be a list")
    if not agg.get("sum_columns") and not agg.get("count_rows", True):
        raise ValueError("Configure sum_columns or set aggregation.count_rows=true")
    opt = config["optimization"]
    partitions = opt.get("shuffle_partitions", 8)
    if isinstance(partitions, bool) or not isinstance(partitions, int) or partitions < 1:
        raise ValueError("optimization.shuffle_partitions must be a positive integer")
    input_cfg = config["input"]
    if input_cfg.get("mode", "demo") not in {"demo", "paths"}:
        raise ValueError("input.mode must be 'demo' or 'paths'")
    if input_cfg.get("mode") == "paths" and (not input_cfg.get("fact_path") or not input_cfg.get("dimension_path")):
        raise ValueError("For input.mode='paths', set input.fact_path and input.dimension_path")
    if input_cfg.get("format", "parquet").lower() not in {"parquet", "delta", "csv", "json"}:
        raise ValueError("input.format must be parquet, delta, csv, or json")
    if config["output"].get("format", "delta") not in {"delta", "parquet"}:
        raise ValueError("output.format must be 'delta' or 'parquet'")
    repeats = config["benchmark"].get("repeats", 3)
    if isinstance(repeats, bool) or not isinstance(repeats, int) or repeats < 1:
        raise ValueError("benchmark.repeats must be a positive integer")
    candidates = config["benchmark"].get("candidates", sorted(CANDIDATES))
    if not isinstance(candidates, list) or not candidates:
        raise ValueError("benchmark.candidates must be a non-empty list")
    unknown = set(candidates) - CANDIDATES
    if unknown:
        raise ValueError(f"Unknown benchmark candidate(s): {sorted(unknown)}")
    filters = config.get("filters", {})
    if not isinstance(filters, dict):
        raise ValueError("filters must be an object")
    for side in ("fact", "dimension"):
        if not isinstance(filters.get(side, []), list):
            raise ValueError(f"filters.{side} must be a list")
        for rule in filters.get(side, []):
            if not isinstance(rule, dict) or not rule.get("column") or rule.get("op") not in FILTER_OPERATORS:
                raise ValueError(f"Invalid {side} filter: {rule!r}")
            if rule["op"] == "isin" and not isinstance(rule.get("value"), list):
                raise ValueError(f"Filter {rule['column']}: isin requires a list value")


def create_demo_data(spark: SparkSession, config: Dict[str, Any]):
    """Create deterministic sample fact and dimension DataFrames."""
    demo = config["demo"]
    fact_rows, dimension_rows = int(demo.get("fact_rows", 200_000)), int(demo.get("dimension_rows", 1_000))
    if fact_rows < 1 or dimension_rows < 1:
        raise ValueError("demo.fact_rows and demo.dimension_rows must be positive")
    key = config["join"]["key"]
    fact = (spark.range(0, fact_rows)
        .withColumn(key, (F.col("id") % dimension_rows).cast("long"))
        .withColumn("quantity", ((F.col("id") % 10) + 1).cast("long"))
        .withColumn("unit_price", ((F.col("id") % 100) + 10).cast("double"))
        .withColumn("sales_amount", F.col("quantity") * F.col("unit_price")))
    dim_key = key
    dimension = (spark.range(0, dimension_rows).withColumnRenamed("id", dim_key)
        .withColumn("category", F.concat(F.lit("Category_"), (F.col(dim_key) % 10).cast("string"))))
    return fact, dimension


def load_input_data(spark: SparkSession, config: Dict[str, Any]):
    """Load either generated demo data or two configured source paths."""
    input_cfg = config["input"]
    if input_cfg.get("mode", "demo") == "demo":
        return create_demo_data(spark, config)
    fmt = input_cfg.get("format", "parquet").lower()
    reader = spark.read.format(fmt)
    if fmt == "csv":
        reader = reader.option("header", input_cfg.get("header", True)).option("inferSchema", input_cfg.get("infer_schema", True))
    return reader.load(input_cfg["fact_path"]), reader.load(input_cfg["dimension_path"])


def _apply_filters(df: DataFrame, rules: List[Dict[str, Any]]) -> DataFrame:
    for rule in rules:
        c, op, value = F.col(rule["column"]), rule["op"], rule.get("value")
        if op == "eq": expr = c == F.lit(value)
        elif op == "ne": expr = c != F.lit(value)
        elif op == "gt": expr = c > F.lit(value)
        elif op == "ge": expr = c >= F.lit(value)
        elif op == "lt": expr = c < F.lit(value)
        elif op == "le": expr = c <= F.lit(value)
        elif op == "isin": expr = c.isin(value)
        elif op == "is_null": expr = c.isNull()
        else: expr = c.isNotNull()
        df = df.filter(expr)
    return df


def _project_inputs(fact: DataFrame, dimension: DataFrame, config: Dict[str, Any]):
    """Apply configured filters early and select columns required by the workload."""
    filters, agg = config.get("filters", {}), config["aggregation"]
    fact = _apply_filters(fact, filters.get("fact", []))
    dimension = _apply_filters(dimension, filters.get("dimension", []))
    key = config["join"]["key"]
    sum_columns = set(agg.get("sum_columns", []))
    group_columns = set(agg.get("group_by", []))
    fact_filter_columns = {r["column"] for r in filters.get("fact", [])}
    dim_filter_columns = {r["column"] for r in filters.get("dimension", [])}
    if key not in fact.columns or key not in dimension.columns:
        raise ValueError(f"Join key {key!r} must exist in both input DataFrames")
    missing_sums = sum_columns - set(fact.columns)
    if missing_sums:
        raise ValueError(f"Fact input is missing sum columns: {sorted(missing_sums)}")
    missing_fact_filters = fact_filter_columns - set(fact.columns)
    missing_dim_filters = dim_filter_columns - set(dimension.columns)
    if missing_fact_filters:
        raise ValueError(f"Fact input is missing filter columns: {sorted(missing_fact_filters)}")
    if missing_dim_filters:
        raise ValueError(f"Dimension input is missing filter columns: {sorted(missing_dim_filters)}")
    missing_groups = group_columns - (set(fact.columns) | set(dimension.columns))
    if missing_groups:
        raise ValueError(f"Group-by columns must exist in at least one input: {sorted(missing_groups)}")
    # Keep group-by columns only on the input where they exist to avoid duplicate
    # ambiguous names after joining. Prefer dimension-side group columns if both have them.
    dim_groups = group_columns & set(dimension.columns)
    fact_groups = group_columns - dim_groups
    fact_needed = {key, *sum_columns, *fact_groups, *fact_filter_columns}
    dim_needed = {key, *dim_groups, *dim_filter_columns}
    return fact.select(*[c for c in fact.columns if c in fact_needed]), dimension.select(*[c for c in dimension.columns if c in dim_needed])


def build_workload(fact: DataFrame, dimension: DataFrame, config: Dict[str, Any], candidate: str = "baseline") -> DataFrame:
    """Build the same business workload with one explicit strategy candidate."""
    validate_config(config)
    if candidate not in CANDIDATES:
        raise ValueError(f"Unknown candidate {candidate!r}; expected one of {sorted(CANDIDATES)}")
    fact_in, dim_in = _project_inputs(fact, dimension, config)
    key, opt = config["join"]["key"], config["optimization"]
    if candidate == "repartition" or (candidate == "configured" and opt.get("repartition_enabled", False)):
        fact_in = fact_in.repartition(int(opt.get("shuffle_partitions", 8)), key)
    if candidate == "broadcast" or (candidate == "configured" and opt.get("broadcast_join", False)):
        dim_in = F.broadcast(dim_in)
    joined = fact_in.join(dim_in, on=key, how=config["join"].get("type", "inner"))
    agg = config["aggregation"]
    expressions = [F.sum(F.col(c)).alias(f"total_{c}") for c in agg.get("sum_columns", [])]
    if agg.get("count_rows", True):
        expressions.append(F.count(F.lit(1)).alias("row_count"))
    groups = agg.get("group_by", [])
    return joined.groupBy(*[F.col(c) for c in groups]).agg(*expressions) if groups else joined.agg(*expressions)


def _plan_text(df: DataFrame) -> str:
    try:
        return df._jdf.queryExecution().executedPlan().toString()
    except Exception as exc:
        return f"Physical plan unavailable: {type(exc).__name__}: {exc}"


def diagnose_plan(plan: str) -> List[Dict[str, str]]:
    """Return cautious hints based on physical-plan text; not full runtime diagnosis."""
    p, findings = plan.lower(), []
    if "sortmergejoin" in p or "sort merge join" in p:
        findings.append({"signal": "Sort-merge join", "interpretation": "A sort/shuffle join appears in the plan.", "recommendation": "Check input sizes and key distribution; compare broadcast only when the dimension side is small enough."})
    if "broadcasthashjoin" in p or "broadcast hash join" in p:
        findings.append({"signal": "Broadcast hash join", "interpretation": "The plan broadcasts one join side.", "recommendation": "Check the broadcast side size and compare measured runtime."})
    if "exchange" in p:
        findings.append({"signal": "Exchange/shuffle marker", "interpretation": "Data redistribution may occur.", "recommendation": "Inspect Spark UI shuffle read/write, spills, and task durations."})
    if "adaptive spark plan" in p or "isfinalplan=false" in p:
        findings.append({"signal": "Adaptive plan marker", "interpretation": "The shown plan may not be the final adaptive plan.", "recommendation": "Inspect the executed plan after the action."})
    if not findings:
        findings.append({"signal": "No common plan pattern matched", "interpretation": "Plan text alone did not reveal a common pattern.", "recommendation": "Use Spark UI/query profile metrics for deeper diagnosis."})
    return findings


def _materialize_and_time(df: DataFrame) -> Dict[str, Any]:
    """Materialize all output columns via cache, then release cache after timing.

    Rebuild the DataFrame for every repeat so cached output from an earlier run
    is not reused as the measured run. count() alone can prune aggregate columns;
    cache materialization helps force evaluation of the complete output schema.
    """
    if StorageLevel is None:
        raise RuntimeError("PySpark is required to run benchmarks")
    cached = df.persist(StorageLevel.MEMORY_AND_DISK)
    start = perf_counter()
    try:
        rows = int(cached.count())
        seconds = perf_counter() - start
    finally:
        cached.unpersist(blocking=True)
    return {"seconds": seconds, "output_rows": rows}


def run_comparison(spark: SparkSession, fact: DataFrame, dimension: DataFrame,
                   config: Dict[str, Any], candidates: Optional[List[str]] = None,
                   repeats: Optional[int] = None) -> Dict[str, Any]:
    """Benchmark candidates, compare output equivalence, and choose fastest valid result."""
    validate_config(config)
    candidates = candidates or config["benchmark"].get("candidates", ["baseline", "broadcast", "repartition", "configured"])
    repeats = int(repeats or config["benchmark"].get("repeats", 3))
    if repeats < 1:
        raise ValueError("repeats must be >= 1")
    unknown = set(candidates) - CANDIDATES
    if not candidates or unknown:
        raise ValueError(f"Candidates must be a non-empty list selected from {sorted(CANDIDATES)}")
    results, frames, plans = [], {}, {}
    for name in candidates:
        durations, counts = [], []
        for _ in range(repeats):
            # Build a fresh plan on every repeat; do not benchmark a cached result.
            df = build_workload(fact, dimension, config, name)
            measured = _materialize_and_time(df)
            durations.append(measured["seconds"])
            counts.append(measured["output_rows"])
        final_df = build_workload(fact, dimension, config, name)
        frames[name], plans[name] = final_df, _plan_text(final_df)
        results.append({"candidate": name, "median_seconds": float(median(durations)),
                        "runs_seconds": durations, "output_rows": counts[-1],
                        "row_counts_stable": len(set(counts)) == 1,
                        "diagnostics": diagnose_plan(plans[name])})
    baseline_name = "baseline" if "baseline" in frames else candidates[0]
    baseline = frames[baseline_name]
    for item in results:
        try:
            candidate = frames[item["candidate"]]
            mismatch = (baseline.exceptAll(candidate).limit(1).count()
                        + candidate.exceptAll(baseline).limit(1).count())
            item["results_match_baseline"] = mismatch == 0
        except Exception as exc:
            item["results_match_baseline"] = None
            item["validation_note"] = f"Output comparison could not complete: {type(exc).__name__}: {exc}"
    base = next((r for r in results if r["candidate"] == baseline_name), results[0])
    for item in results:
        item["improvement_vs_baseline_percent"] = (
            (base["median_seconds"] - item["median_seconds"]) / base["median_seconds"] * 100
            if base["median_seconds"] > 0 else None)
    valid = [r for r in results if r.get("results_match_baseline") is True and r["row_counts_stable"]]
    best = min(valid, key=lambda r: r["median_seconds"]) if valid else None
    return {"candidates": results, "best_validated_candidate": best["candidate"] if best else None,
            "best_validated_median_seconds": best["median_seconds"] if best else None,
            "baseline_candidate": baseline_name, "repeats": repeats, "plans": plans,
            "dataframes": frames,
            "warning": "Results depend on data, runtime, compute, warm-up and concurrent activity. Plan hints are heuristic, not a full diagnosis."}


def write_output(df: DataFrame, config: Dict[str, Any]) -> None:
    """Write a selected result only when explicitly configured by the user."""
    output = config["output"]
    if output.get("table"):
        df.write.format(output.get("format", "delta")).mode(output.get("mode", "overwrite")).saveAsTable(output["table"])
    elif output.get("path"):
        df.write.format(output.get("format", "delta")).mode(output.get("mode", "overwrite")).save(output["path"])
    else:
        raise ValueError("Set output.table or output.path in the output configuration")
