"""One-shot Spark session to cache Kafka connector JARs."""
from pyspark.sql import SparkSession

SparkSession.builder.appName("JarPrefetch").master("local[1]").getOrCreate().stop()
