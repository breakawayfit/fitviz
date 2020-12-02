import os

import numpy as np
import pandas as pd
import streamlit as st

from pyspark.sql import DataFrame, SparkSession

spark: SparkSession = (
    SparkSession.builder.master("local[*]")
    .appName("Fitviz Streamlit Applitcation Dev")
    .config("spark.driver.memory", "8g")
    .config(
        "spark.jars.packages",
        "io.delta:delta-core_2.12:0.7.0,org.apache.hadoop:hadoop-aws:2.7.7",
    )
    .config(
        "spark.delta.logStore.class",
        "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    )
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", None))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", None))
    .getOrCreate()
)

from delta.tables import DeltaTable

df: DataFrame = spark.createDataFrame(np.random.randn(200, 3).tolist(), ["a", "b", "c"])
pdf: pd.DataFrame = df.toPandas()

st.title("Streamlit Starter")
st.header("Vega Lite Chart")
st.vega_lite_chart(
    pdf,
    {
        "mark": {"type": "circle", "tooltip": True},
        "encoding": {
            "x": {"field": "a", "type": "quantitative"},
            "y": {"field": "b", "type": "quantitative"},
            "size": {"field": "c", "type": "quantitative"},
            "color": {"field": "c", "type": "quantitative"},
        },
    },
)
