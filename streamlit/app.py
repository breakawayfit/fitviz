import numpy as np
import pandas as pd
import streamlit as st

from pyspark.sql import DataFrame, SparkSession

spark: SparkSession = SparkSession.builder.appName("Streamlit App").getOrCreate()

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

