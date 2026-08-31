#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pyspark==3.5.5",
#     "delta-spark==3.3.2",
# ]
# ///
"""Builds the Spark-written Delta tables under crates/ldrs-test-fixtures/test_data/delta_spark.

    uv run scripts/spark/build_fixtures.py penguins_partitioned
"""

import shutil
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

REPO = Path(__file__).resolve().parents[2]
DATA = REPO / "crates" / "ldrs-test-fixtures" / "test_data"
FIXTURES = DATA / "delta_spark"
PENGUIN_CSVS = sorted((DATA / "duckdb").glob("penguins_*.csv"))

# Explicit casts so the schema does not move with the data; "NA" becomes null through the numeric
# ones. Each raw header is paired with the name `snake_case` gives it.
COLUMNS = [
    ("studyName", "study_name", "string"),
    ("Sample Number", "sample_number", "int"),
    ("Species", "species", "string"),
    ("Region", "region", "string"),
    ("Island", "island", "string"),
    ("Stage", "stage", "string"),
    ("Individual ID", "individual_id", "string"),
    ("Clutch Completion", "clutch_completion", "string"),
    ("Date Egg", "date_egg", "date"),
    ("Culmen Length (mm)", "culmen_length_mm", "double"),
    ("Culmen Depth (mm)", "culmen_depth_mm", "double"),
    ("Flipper Length (mm)", "flipper_length_mm", "int"),
    ("Body Mass (g)", "body_mass_g", "int"),
    ("Sex", "sex", "string"),
    ("Delta 15 N (o/oo)", "delta_15_n", "double"),
    ("Delta 13 C (o/oo)", "delta_13_c", "double"),
    ("Comments", "comments", "string"),
]


def penguins(spark: SparkSession) -> DataFrame:
    raw = spark.read.csv(
        [str(path) for path in PENGUIN_CSVS], header=True, inferSchema=False
    )
    return raw.select(
        *[
            F.col(f"`{raw_name}`").cast(kind).alias(raw_name)
            for raw_name, _, kind in COLUMNS
        ]
    )


def snake_case(frame: DataFrame) -> DataFrame:
    return frame.select(
        *[F.col(f"`{raw_name}`").alias(name) for raw_name, name, _ in COLUMNS]
    )


def penguins_partitioned(spark: SparkSession, path: Path) -> None:
    frame = snake_case(penguins(spark)).cache()
    for study in studies(frame, "study_name"):
        (
            frame.where(F.col("study_name") == study)
            .repartition(1)
            .write.format("delta")
            .partitionBy("island")
            .mode("append")
            .save(str(path))
        )


def penguins_column_mapped(spark: SparkSession, path: Path) -> None:
    frame = penguins(spark).cache()
    for study in studies(frame, "studyName"):
        (
            frame.where(F.col("studyName") == study)
            .repartition(1)
            .write.format("delta")
            .option("delta.columnMapping.mode", "name")
            .mode("append")
            .save(str(path))
        )


def penguins_column_mapped_id(spark: SparkSession, path: Path) -> None:
    (
        penguins(spark)
        .repartition(1)
        .write.format("delta")
        .option("delta.columnMapping.mode", "id")
        .mode("append")
        .save(str(path))
    )


def penguins_row_tracking_clustered(spark: SparkSession, path: Path) -> None:
    (
        DeltaTable.create(spark)
        .location(str(path))
        .addColumns(snake_case(penguins(spark)).schema)
        .clusterBy("sample_number")
        .property("delta.enableRowTracking", "true")
        .execute()
    )


def penguins_iceberg_compat(spark: SparkSession, path: Path) -> None:
    (
        snake_case(penguins(spark))
        .limit(0)
        .write.format("delta")
        .option("delta.columnMapping.mode", "name")
        .option("delta.enableIcebergCompatV2", "true")
        .mode("append")
        .save(str(path))
    )


def penguins_partitioned_nulls(spark: SparkSession, path: Path) -> None:
    frame = (
        snake_case(penguins(spark))
        .select(
            "study_name",
            "sample_number",
            "island",
            # "NA" means unrecorded, which a partition value has to spell as a real null.
            F.when(F.col("sex") != "NA", F.col("sex")).alias("sex"),
            "body_mass_g",
        )
        .cache()
    )
    for study in studies(frame, "study_name")[:2]:
        (
            frame.where(F.col("study_name") == study)
            .repartition(1)
            .write.format("delta")
            .partitionBy("island", "sex")
            .mode("append")
            .save(str(path))
        )


def penguins_in_commit_timestamps(spark: SparkSession, path: Path) -> None:
    frame = snake_case(penguins(spark)).cache()
    for study in studies(frame, "study_name")[:2]:
        (
            frame.where(F.col("study_name") == study)
            .limit(40)
            .repartition(1)
            .write.format("delta")
            .option("delta.enableInCommitTimestamps", "true")
            .mode("append")
            .save(str(path))
        )


def studies(frame: DataFrame, column: str) -> list[str]:
    return [
        row[0] for row in frame.select(column).distinct().sort(column).collect()
    ]


def build(spark: SparkSession, name: str) -> None:
    path = FIXTURES / name
    shutil.rmtree(path, ignore_errors=True)

    match name:
        case "penguins_partitioned":
            penguins_partitioned(spark, path)
        case "penguins_column_mapped":
            penguins_column_mapped(spark, path)
        case "penguins_column_mapped_id":
            penguins_column_mapped_id(spark, path)
        case "penguins_row_tracking_clustered":
            penguins_row_tracking_clustered(spark, path)
        case "penguins_partitioned_nulls":
            penguins_partitioned_nulls(spark, path)
        case "penguins_in_commit_timestamps":
            penguins_in_commit_timestamps(spark, path)
        case "penguins_iceberg_compat":
            penguins_iceberg_compat(spark, path)
        case _:
            raise ValueError(f"unknown fixture '{name}'")

    # Hadoop's LocalFileSystem writes a hidden .crc beside every file, which no table on object
    # storage has. Delta's own _delta_log/*.crc checksums are not dot-prefixed and stay.
    for sidecar in path.rglob(".*.crc"):
        sidecar.unlink()

    commits = sorted((path / "_delta_log").glob("*.json"))
    files = [p for p in path.rglob("*.parquet") if "_delta_log" not in p.parts]
    print(f"{name}: {len(commits)} commits, {len(files)} data files", flush=True)


def main() -> int:
    builder = (
        SparkSession.builder.appName("ldrs-fixtures")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "1")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        FIXTURES.mkdir(parents=True, exist_ok=True)
        for name in sys.argv[1:]:
            build(spark, name)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
