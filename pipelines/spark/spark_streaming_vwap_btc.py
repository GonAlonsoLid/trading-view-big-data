# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    sum as _sum,
    window,
    struct,
    to_json,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)


BOOTSTRAP_SERVERS = "192.168.80.34:9092"
INPUT_TOPIC = "gittba_BTC"
OUTPUT_TOPIC = "gittba_BTC_VWAP"
WINDOW_DURATION = "5 minutes"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("VWAP_BTC_Streaming")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Schema del JSON que llega del producer
    value_schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("@timestamp", StringType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False),
    ])

    # Leer del topic de Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parsear el JSON del value
    parsed_df = (
        raw_df
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
        .select(
            col("key"),
            from_json(col("value"), value_schema).alias("data"),
            col("timestamp").alias("kafka_ts"),
        )
        .filter(col("data").isNotNull())
        .select(
            col("key"),
            col("data.symbol").alias("symbol"),
            col("data.close").alias("close"),
            col("data.volume").alias("volume"),
            col("kafka_ts"),
        )
    )

    # Watermark para que Spark cierre ventanas y libere estado
    parsed_df = parsed_df.withWatermark("kafka_ts", "10 seconds")

    # Calcular precio * volumen para el VWAP
    with_pv = parsed_df.withColumn("price_volume", col("close") * col("volume"))

    # VWAP por ventana de 5 minutos
    vwap_df = (
        with_pv
        .groupBy(
            window(col("kafka_ts"), WINDOW_DURATION),
            col("symbol"),
        )
        .agg(
            (_sum("price_volume") / _sum("volume")).alias("vwap"),
        )
        .select(
            col("symbol"),
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("vwap"),
        )
    )

    # Formatear como JSON y publicar en topic de salida
    output_df = (
        vwap_df
        .select(
            col("symbol").alias("key"),
            to_json(
                struct("window_start", "window_end", "symbol", "vwap")
            ).alias("value"),
        )
    )

    query = (
        output_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", "/tmp/vwap_btc_checkpoint")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
