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
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

orders = (
    spark.read.parquet("s3a://de-course/staging/stg_orders")
    .filter((F.col("order_date") >= window_start) & (F.col("order_date") < load_date))
)

payments = (
    spark.read.parquet("s3a://de-course/staging/stg_payments")
    .filter(
        ((F.col("paid_dt") >= window_start) & (F.col("paid_dt") < load_date))
        | F.col("paid_dt").isNull()
    )
)


success_not_refund = (F.col("payment_status") == "success") & (~F.col("is_refund"))

order_payments = (
    payments
    .groupBy("order_id")
    .agg(
        F.count(F.when(success_not_refund, 1)).alias("successful_payments_count"),
        F.sum(F.when(success_not_refund, F.col("amount"))).alias("paid_amount"),
        (-F.sum(F.when(F.col("is_refund"), F.col("amount")))).alias("refunded_amount"),
        F.min(F.when(success_not_refund, F.col("paid_at"))).alias("first_paid_at"),
        F.count(F.when(F.col("payment_status") == "success", 1)).alias("success_operations_count"),
        F.count(F.when(F.col("payment_status") == "failed", 1)).alias("failed_operations_count"),
        F.count(F.when(F.col("payment_status") == "pending", 1)).alias("pending_operations_count"),
    )
)

result = (
    orders.alias("o")
    .join(order_payments.alias("p"), on="order_id", how="left")
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("o.ordered_at"),
        F.col("o.order_date"),
        F.col("o.order_status"),
        F.col("o.order_amount"),
        F.col("o.is_test_order"),

        F.coalesce(F.col("p.successful_payments_count"), F.lit(0)).alias("successful_payments_count"),
        F.coalesce(F.col("p.paid_amount"), F.lit(0)).alias("paid_amount"),
        F.coalesce(F.col("p.refunded_amount"), F.lit(0)).alias("refunded_amount"),

        (
            F.coalesce(F.col("p.paid_amount"), F.lit(0))
            - F.coalesce(F.col("p.refunded_amount"), F.lit(0))
        ).alias("net_paid_amount"),

        F.col("p.first_paid_at"),

        (
            (F.col("o.order_status") == "paid")
            & (F.coalesce(F.col("p.successful_payments_count"), F.lit(0)) > 0)
        ).alias("is_confirmed"),

        F.coalesce(F.col("p.success_operations_count"), F.lit(0)).alias("success_operations_count"),
        F.coalesce(F.col("p.failed_operations_count"), F.lit(0)).alias("failed_operations_count"),
        F.coalesce(F.col("p.pending_operations_count"), F.lit(0)).alias("pending_operations_count"),
    )
)


(
    result.write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet("s3a://de-course/intermediate/int_orders_with_payments")
)

print(f"Сохранено: s3a://de-course/intermediate/int_orders_with_payments, строк: {result.count()}")
spark.stop()