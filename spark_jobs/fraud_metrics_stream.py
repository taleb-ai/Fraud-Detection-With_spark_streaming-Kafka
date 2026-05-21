"""Real-time fraud detection metrics — Spark Structured Streaming (TP5 §2.1, §5.2)."""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import (
    ALERTS_PATH,
    ALERT_AMOUNT_MULTIPLIER,
    ALERT_TX_COUNT_3H,
    CHECKPOINT_DIR,
    KAFKA_BOOTSTRAP,
    KAFKA_PACKAGE,
    KAFKA_STARTING_OFFSETS,
    KAFKA_TOPIC,
    LATEST_SNAPSHOT_PATH,
    LIFETIME_PATH,
    MAX_OFFSETS_PER_TRIGGER,
    METRICS_DIR,
    RAW_RECENT_PATH,
    TRIGGER_INTERVAL,
    WATERMARK_DELAY,
    WINDOW_SPECS,
)

BATCH_LOG = os.path.join(os.path.dirname(METRICS_DIR), "spark-batch.log")


def _log_batch(message: str, **fields) -> None:
    os.makedirs(os.path.dirname(BATCH_LOG), exist_ok=True)
    parts = [f"{datetime.now(timezone.utc).isoformat()}", message]
    for k, v in fields.items():
        parts.append(f"{k}={v}")
    line = " ".join(parts) + "\n"
    with open(BATCH_LOG, "a", encoding="utf-8") as f:
        f.write(line)

TX_SCHEMA = StructType(
    [
        StructField("msg_entity", StringType()),
        StructField("app_type", StringType()),
        StructField("send_entity", StringType()),
        StructField("receive_entity", StringType()),
        StructField("send_id", StringType()),
        StructField("receive_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("date", StringType()),
        StructField("tx_type", StringType()),
        StructField("tx_id", StringType()),
        StructField("sim_fraud", BooleanType(), True),
    ]
)

def build_spark(app_name: str = "FraudMetricsStream") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "24"))
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.jars.packages", KAFKA_PACKAGE)
    )
    master = os.environ.get("SPARK_MASTER")
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def read_transactions(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_value", "timestamp AS kafka_ingest_time")
        .select(F.from_json("json_value", TX_SCHEMA).alias("tx"), "kafka_ingest_time")
        .select("tx.*", "kafka_ingest_time")
        .withColumn("event_time", F.to_timestamp("date"))
        .filter(F.col("send_id").isNotNull() & F.col("receive_id").isNotNull())
        .withWatermark("event_time", WATERMARK_DELAY)
    )
    return parsed


def _windowed_metrics(events: DataFrame, user_col: str, peer_col: str, direction: str) -> DataFrame:
    """Build union of all windowed aggregations for sent or received."""
    parts = []
    for window_name, duration, slide in WINDOW_SPECS:
        agg = (
            events.groupBy(
                F.window(F.col("event_time"), duration, slide).alias("w"),
                F.col(user_col).alias("user_id"),
            )
            .agg(
                F.avg("amount").alias("avg_amount"),
                F.count(F.lit(1)).alias("tx_count"),
                F.approx_count_distinct(peer_col).alias("distinct_peers"),
            )
            .select(
                "user_id",
                F.lit(direction).alias("direction"),
                F.lit(window_name).alias("window_name"),
                F.col("w.start").alias("window_start"),
                F.col("w.end").alias("window_end"),
                "avg_amount",
                F.col("tx_count").cast("double").alias("tx_count"),
                F.col("distinct_peers").cast("double").alias("distinct_peers"),
                F.current_timestamp().alias("computed_at"),
            )
        )
        parts.append(agg)

    result = parts[0]
    for p in parts[1:]:
        result = result.unionByName(p)
    return result


def build_metrics_stream(events: DataFrame) -> DataFrame:
    sent = events.select(
        "send_id",
        "receive_id",
        "amount",
        "event_time",
        "date",
        "tx_id",
        "sim_fraud",
    )
    received = events.select(
        F.col("receive_id").alias("send_id"),
        F.col("send_id").alias("receive_id"),
        "amount",
        "event_time",
        "date",
        "tx_id",
        "sim_fraud",
    )

    sent_metrics = _windowed_metrics(sent, "send_id", "receive_id", "sent")
    recv_metrics = _windowed_metrics(received, "send_id", "receive_id", "received")
    return sent_metrics.unionByName(recv_metrics)


def _merge_lifetime(batch_df: DataFrame, direction: str, user_col: str, peer_col: str) -> DataFrame:
    batch_agg = (
        batch_df.groupBy(F.col(user_col).alias("user_id"))
        .agg(
            F.count(F.lit(1)).alias("tx_count"),
            F.sum("amount").alias("total_amount"),
            F.approx_count_distinct(peer_col).alias("distinct_peers"),
        )
        .withColumn("direction", F.lit(direction))
    )
    return batch_agg


def _write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    df.write.mode(mode).parquet(path)


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df is None or batch_df.rdd.isEmpty():
        _log_batch("process_batch_empty", batch_id=batch_id)
        return

    row_count = batch_df.count()
    os.makedirs(METRICS_DIR, exist_ok=True)
    os.makedirs(LIFETIME_PATH, exist_ok=True)

    spark = batch_df.sparkSession
    now = datetime.now(timezone.utc)

    # Recent raw transactions for dashboard "last 10 seconds"
    recent = batch_df.withColumn("computed_at", F.lit(now).cast("timestamp"))
    _write_parquet(recent, RAW_RECENT_PATH)

    # Lifetime merge (sent + received)
    for direction, user_col, peer_col in (
        ("sent", "send_id", "receive_id"),
        ("received", "receive_id", "send_id"),
    ):
        part = _merge_lifetime(batch_df, direction, user_col, peer_col)
        out_dir = os.path.join(LIFETIME_PATH, direction)
        tmp_dir = os.path.join(LIFETIME_PATH, f"_tmp_{direction}_{batch_id}")

        if os.path.exists(out_dir):
            existing = spark.read.parquet(out_dir)
            merged = (
                existing.unionByName(part)
                .groupBy("user_id", "direction")
                .agg(
                    F.sum("tx_count").alias("tx_count"),
                    F.sum("total_amount").alias("total_amount"),
                    F.max("distinct_peers").alias("distinct_peers"),
                )
                .withColumn("computed_at", F.lit(now).cast("timestamp"))
            )
        else:
            merged = part.withColumn("computed_at", F.lit(now).cast("timestamp"))

        _write_parquet(merged, tmp_dir)
        # atomic-ish replace
        import shutil

        if os.path.exists(out_dir):
            shutil.rmtree(out_dir)
        os.rename(tmp_dir, out_dir)

    alerts = (
        batch_df.groupBy("send_id")
        .agg(
            F.count(F.lit(1)).alias("tx_count"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.sum(F.when(F.col("sim_fraud") == True, 1).otherwise(0)).alias("sim_fraud_count"),
        )
        .filter(
            (F.col("tx_count") > ALERT_TX_COUNT_3H / 120)  # per ~10s batch threshold
            | (F.col("max_amount") > ALERT_AMOUNT_MULTIPLIER * F.col("avg_amount"))
            | (F.col("sim_fraud_count") > 0)
        )
        .withColumn("computed_at", F.lit(now).cast("timestamp"))
        .withColumnRenamed("send_id", "user_id")
    )
    if alerts.count() > 0:
        _write_parquet(alerts, ALERTS_PATH)

    _log_batch(
        "process_batch_ok",
        batch_id=batch_id,
        rows=row_count,
        path=RAW_RECENT_PATH,
    )


def write_metrics_batch(metrics_df: DataFrame, batch_id: int) -> None:
    if metrics_df is None or metrics_df.rdd.isEmpty():
        _log_batch("write_metrics_empty", batch_id=batch_id)
        return
    os.makedirs(METRICS_DIR, exist_ok=True)
    metrics_df = metrics_df.withColumn("batch_id", F.lit(batch_id))

    batch_path = os.path.join(METRICS_DIR, "batches", f"batch_{batch_id:06d}")
    metrics_df.write.mode("overwrite").partitionBy("window_name", "direction").parquet(batch_path)
    _write_parquet(metrics_df, LATEST_SNAPSHOT_PATH)
    _log_batch(
        "write_metrics_ok",
        batch_id=batch_id,
        rows=metrics_df.count(),
        path=LATEST_SNAPSHOT_PATH,
    )


def main() -> None:
    os.makedirs(METRICS_DIR, exist_ok=True)
    _log_batch(
        "job_start",
        metrics_dir=METRICS_DIR,
        kafka=KAFKA_BOOTSTRAP,
        topic=KAFKA_TOPIC,
        offsets=KAFKA_STARTING_OFFSETS,
        trigger=TRIGGER_INTERVAL,
    )

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    events = read_transactions(spark)
    metrics_stream = build_metrics_stream(events)

    metrics_query = (
        metrics_stream.writeStream.foreachBatch(write_metrics_batch)
        .outputMode("update")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "windowed-metrics"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    events_query = (
        events.writeStream.foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "raw-events"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("Fraud metrics streaming job started.")
    print(f"  Kafka: {KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC}")
    print(f"  Metrics: {METRICS_DIR}")
    print(f"  Checkpoint: {CHECKPOINT_DIR}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
