# Spark Job Performance Engine

A reusable Databricks framework that benchmarks different Spark performance strategies, validates output correctness, and identifies the fastest valid strategy for a join-and-aggregation workload.

The framework uses a configuration-driven approach, allowing users to customize workloads, input data, execution strategies, and benchmark settings through `config/config.json`, without modifying the core Python code.

## Features

* Configuration-driven workload execution.
* Generated sample data or custom input data (`Parquet`, `Delta`, `CSV`, `JSON`).
* Multiple Execution strategies: `baseline`, `broadcast`, `repartition`, and `configured`.
* Repeated benchmarks with median execution times.
* Output validation against the baseline.

## Project Structure

```text
Spark\_Job\_Performance\_Engine/
├── config/
│   └── config.json
├── src/
│   └── tuning\_engine.py
├── notebooks/
│   └── Spark\_Job\_Performance.py
└── README.md
```

## Performance Strategies

|Strategy|Description|
|-|-|
|`baseline`|Uses Spark's default execution planning.|
|`broadcast`|Applies a broadcast hint to the dimension data.|
|`repartition`|Repartitions fact data using the join key.|
|`configured`|Applies optimization settings from the configuration file.|

All strategies perform the same business calculation while testing different execution approaches.

## Configuration

Update `config/config.json` to customize:

* **Input:** Sample data or custom file paths and formats.
* **Join:** Join key and join type.
* **Aggregation:** Grouping columns, sum columns, and row counts.
* **Filters:** Optional fact and dimension data filters.
* **Techniques:** Broadcast and repartition settings.
* **Benchmark:** Number of repetitions and strategies to run.
* **Output:** Whether to save results and the destination.

## Setup and Execution

1. Upload the complete project folder to your Databricks workspace.
2. Keep the `config`, `src`, and `notebooks` folders together.
3. Update `config/config.json` according to your requirements.
4. Open `notebooks/Spark\_Job\_Performance.py`.
5. Run all cells using **Run All**.
6. Review the benchmark results, validation status, fastest strategy, and execution plan hints.

## Prerequisites

* Databricks workspace with PySpark.
* Read access to input data when using custom data paths.
* Write permissions if result saving is enabled.

## Technologies

* Python
* PySpark
* Apache Spark
* Databricks
* Delta Lake

## Sources

* \[Apache Spark / PySpark]([https://github.com/apache/spark](https://github.com/apache/spark))
* \[Delta Lake]([https://github.com/delta-io/delta](https://github.com/delta-io/delta))

