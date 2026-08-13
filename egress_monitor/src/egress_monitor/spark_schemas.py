"""Explicit Spark schemas for the output tables (in-workspace SparkSink only).

Imported lazily by SparkSink so the standalone/file path never needs pyspark.
Explicit (not inferred): several fields are nullable and an all-null column or
an empty batch would otherwise make Spark infer the wrong type or fail.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .sinks import (
    API_USAGE_TABLE,
    AUDIT_TABLE,
    LINEAGE_TABLE,
    NOTEBOOK_TABLE,
)

_RUN_FIELDS = [
    StructField("run_id", StringType(), False),
    StructField("run_ts", TimestampType(), False),
]

SCHEMAS: dict[str, StructType] = {
    NOTEBOOK_TABLE: StructType([
        StructField("path", StringType(), True),
        StructField("language", StringType(), True),
        StructField("object_id", LongType(), True),
        StructField("modified_at", TimestampType(), True),
        StructField("cell_no", IntegerType(), True),
        StructField("line_no", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("pattern_name", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("snippet", StringType(), True),
        *_RUN_FIELDS,
    ]),
    AUDIT_TABLE: StructType([
        StructField("event_time", TimestampType(), True),
        StructField("email", StringType(), True),
        StructField("service_name", StringType(), True),
        StructField("action_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("source_ip_address", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("request_summary", StringType(), True),
        *_RUN_FIELDS,
    ]),
    API_USAGE_TABLE: StructType([
        StructField("event_date", DateType(), True),
        StructField("email", StringType(), True),
        StructField("service_name", StringType(), True),
        StructField("action_name", StringType(), True),
        StructField("call_count", LongType(), True),
        StructField("baseline_mean", DoubleType(), True),
        StructField("baseline_stddev", DoubleType(), True),
        StructField("zscore", DoubleType(), True),
        StructField("is_spike", BooleanType(), True),
        *_RUN_FIELDS,
    ]),
    LINEAGE_TABLE: StructType([
        StructField("event_time", TimestampType(), True),
        StructField("email", StringType(), True),
        StructField("entity_type", StringType(), True),
        StructField("source_type", StringType(), True),
        StructField("source", StringType(), True),
        StructField("target_type", StringType(), True),
        StructField("target", StringType(), True),
        StructField("direction", StringType(), True),
        *_RUN_FIELDS,
    ]),
}
