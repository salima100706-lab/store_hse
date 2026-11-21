#!/bin/bash

spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --executor-cores 1 \
  --executor-memory 1g \
  --driver-memory 1g \
  --conf spark.dynamicAllocation.enabled=false \
  product_categories.py "$@"