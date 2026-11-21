import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True)
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
window_start = load_date - timedelta(days=3)

spark = (
    SparkSession.builder
    .appName("load-fct-orders")
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

# 1. Читаем источник с фильтром по партициям за окно
df = (
    spark.read.parquet("s3a://de-course/intermediate/int_orders_with_payments")
    .filter((F.col("order_date") >= window_start) & (F.col("order_date") < load_date))
)

# 2. Фильтр (без тестовых) -> Проекция (12 колонок)
result = (
    df.filter(~F.col("is_test_order"))
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "ordered_at",
        "order_status",
        "order_amount",
        "paid_amount",
        "refunded_amount",
        "net_paid_amount",
        "successful_payments_count",
        "first_paid_at",
        "is_confirmed",
    )
)

# 3. Запись
(
    result.write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet("s3a://de-course/marts/fct_orders")
)

print(f"Сохранено: s3a://de-course/marts/fct_orders, строк: {result.count()}")
spark.stop()