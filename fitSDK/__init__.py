from __future__ import absolute_import

from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

ROOT = Path(__file__).parent


def _get_profile_lookup() -> Dict[str, List[str]]:
    profile_pdf = pd.read_excel(
        Path(ROOT, "Profile.xlsx"),
        names=["subset", "base", "value", "to_replace", "comment"],
        dtype={"subset": np.str, "value": np.str, "to_replace": np.str},
    )

    profile_pdf[["subset", "base"]] = profile_pdf[["subset", "base"]].fillna(
        method="ffill"
    )
    is_def_row = profile_pdf["value"].isna() & profile_pdf["to_replace"].isna()

    profile_pdf = profile_pdf[~is_def_row]

    enum_replace_dict = (
        profile_pdf[["subset", "to_replace", "value"]]
        .groupby("subset")
        .agg(list)
        .to_dict(orient="index")
    )

    enum_replace_dict["device_type"] = enum_replace_dict["antplus_device_type"]
    return enum_replace_dict


def _get_enum_xref() -> pd.DataFrame:
    profile_pdf = pd.read_excel(
        Path(ROOT, "Profile.xlsx"),
        names=["field", "typ", "enum_value", "value", "comment"],
        dtype={
            "field": np.str,
            "typ": np.str,
            "enum_value": np.str,
            "value": np.str,
            "comment": np.str,
        },
    )

    profile_pdf[["field", "typ"]] = profile_pdf[["field", "typ"]].fillna(method="ffill")
    is_def_row = profile_pdf["enum_value"].isna() & profile_pdf["value"].isna()

    profile_pdf = profile_pdf[~is_def_row]
    return SparkSession.builder.getOrCreate().createDataFrame(
        profile_pdf.astype(np.str)
    )


PROFILE = _get_enum_xref()