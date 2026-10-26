# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Spark_Job_Performance
# MAGIC A reusable, configuration-driven PySpark example. Change `config/config.json` to adapt the workload, input paths, filters, aggregation, candidates, and output. This benchmarks a configured workload; it does not rewrite arbitrary Spark jobs.

# COMMAND ----------

# Cell 1: Resolve project files and load the JSON configuration.
# DEFAULT: keep both values empty to auto-detect config/ and src/ from the project folder.
# If auto-detection fails in your Databricks Workspace, paste the full file paths here.
CONFIG_PATH = ""  # Example: "/Workspace/Users/you/Spark_Job_Performance_Tuning/config/config.json"
ENGINE_PATH = ""  # Example: "/Workspace/Users/you/Spark_Job_Performance_Tuning/src/tuning_engine.py"

from pathlib import Path
import importlib.util
import json
import os

notebook_path = ""
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
except Exception:
    pass

# Expected Workspace layout: project-root/{config,src,notebooks}
project_root = str(Path(notebook_path).parent.parent) if notebook_path else ""

def workspace_file(relative_path):
    if project_root:
        return "/Workspace" + project_root + "/" + relative_path
    return ""

if not CONFIG_PATH:
    CONFIG_PATH = workspace_file("config/config.json")
if not ENGINE_PATH:
    ENGINE_PATH = workspace_file("src/tuning_engine.py")

if not CONFIG_PATH or not os.path.isfile(CONFIG_PATH):
    raise FileNotFoundError(
        "Cannot locate config/config.json. Upload the complete project folder, "
        "then set CONFIG_PATH at the top of Cell 1 to its full Workspace path."
    )
if not ENGINE_PATH or not os.path.isfile(ENGINE_PATH):
    raise FileNotFoundError(
        "Cannot locate src/tuning_engine.py. Upload the complete project folder, "
        "then set ENGINE_PATH at the top of Cell 1 to its full Workspace path."
    )

spec = importlib.util.spec_from_file_location("spark_tuning_engine", ENGINE_PATH)
engine = importlib.util.module_from_spec(spec)
spec.loader.exec_module(engine)
CONFIG = engine.load_config(CONFIG_PATH)
print("Project:", CONFIG.get("project_name", "Spark_Job_Performance_Tuning"))
print("Config loaded from:", CONFIG_PATH)
print("Reusable engine loaded from:", ENGINE_PATH)

# COMMAND ----------

# Cell 2: Load demo or user-provided input DataFrames.
from pyspark.sql import Row

fact_df, dimension_df = engine.load_input_data(spark, CONFIG)
print("Fact input columns:", fact_df.columns)
print("Dimension input columns:", dimension_df.columns)
display(fact_df.limit(5))
display(dimension_df.limit(5))

# COMMAND ----------

# Cell 3: Benchmark the configured candidates and validate output equivalence.
comparison = engine.run_comparison(spark, fact_df, dimension_df, CONFIG)
results = comparison["candidates"]
result_frames = comparison["dataframes"]
plan_texts = comparison["plans"]
best_name = comparison["best_validated_candidate"]
metrics_df = spark.createDataFrame([
    Row(
        candidate=r["candidate"],
        median_seconds=float(r["median_seconds"]),
        runs_seconds=str([round(x, 4) for x in r["runs_seconds"]]),
        output_rows=int(r["output_rows"]),
        row_counts_stable=bool(r["row_counts_stable"]),
        results_match_baseline=str(r.get("results_match_baseline")),
        improvement_vs_baseline_percent=(float(r["improvement_vs_baseline_percent"])
            if r.get("improvement_vs_baseline_percent") is not None else None),
    ) for r in results
])
print("Candidate benchmark results:")
display(metrics_df)

# COMMAND ----------

# Cell 4: Baseline vs best validated candidate. Never label an invalid candidate as the winner.
baseline_name = comparison["baseline_candidate"]
base_result = next(r for r in results if r["candidate"] == baseline_name)
if best_name:
    selected = next(r for r in results if r["candidate"] == best_name)
    print("Best validated candidate:", best_name)
else:
    selected = None
    print("WARNING: No candidate passed output-equivalence and stable-row-count validation.")
    print("No tuned winner will be selected or saved.")

summary = [Row(
    job_type="baseline", candidate=baseline_name,
    execution_time_seconds=float(base_result["median_seconds"]),
    output_rows=int(base_result["output_rows"]),
    validation=str(base_result.get("results_match_baseline")),
)]
if selected:
    summary.append(Row(
        job_type="best_validated", candidate=best_name,
        execution_time_seconds=float(selected["median_seconds"]),
        output_rows=int(selected["output_rows"]),
        validation=str(selected.get("results_match_baseline")),
    ))
display(spark.createDataFrame(summary))
if selected and base_result["median_seconds"] > 0:
    improvement = (base_result["median_seconds"] - selected["median_seconds"]) / base_result["median_seconds"] * 100
    print(f"Measured time improvement vs baseline: {improvement:.2f}%")
#print(comparison["warning"])

# COMMAND ----------

# Cell 6: Optional output write. Disabled in config by default.
output = CONFIG["output"]
if output.get("save_result", False):
    if not best_name:
        raise RuntimeError("No validated candidate is available; refusing to save output.")
    if not output.get("table") and not output.get("path"):
        raise ValueError("Set output.table or output.path in config/config.json before saving.")
    engine.write_output(result_frames[best_name], CONFIG)
    print("Saved validated candidate:", best_name)
else:
    print("Output write skipped. Set output.save_result=true only after checking the destination and write mode.")