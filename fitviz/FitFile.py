import datetime
from logging import setLoggerClass
import subprocess

from collections import defaultdict, namedtuple
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union


from loguru import logger
from pyspark.sql import Column, DataFrame, SparkSession, functions as F, Window

import fitSDK
from fitSDK import PROFILE

FIT_SDK_DIR = Path(fitSDK.__file__).parent
FIT_SDK_PATH = Path(FIT_SDK_DIR, "FitCSVTool.jar")

NO_HOME_ASSERT_MSG = """
$HOME directory is not set.
Set HOME environment variable or
Define temporary directory for processing
"""

# .fit file header names
FIELD_ATTRS = ("Field", "Value", "Units")
RECORD_ATTRS = ("record_type", "Local Number", "Message")


class FitFile:
    def __init__(
        self,
        path: str,
        temp_dir: Optional[Union[str, Path]] = None,
        reprocess: bool = False,
    ):
        self._spark = SparkSession.builder.getOrCreate()
        self._reprocess = reprocess
        if temp_dir is not None:
            self._temp_dir_path: Path = Path(temp_dir)
        else:
            self._temp_dir_path = self._get_default_temp_dir()

        self._init_logger()
        self.fit_path: Path = Path(path)  # TODO check fit type
        self.csv_path: Path = self._init_csv_path()
        self.csv_df = self._init_csv_df()
        self.long_df = self._expode_field_arrays()

        # self.message_dfs = self._get_message_dataframes()

    def _init_logger(self):
        log_dir = Path(self._temp_dir_path, "logs")
        if not log_dir.exists():
            log_dir.mkdir()
        logfile_path = Path(log_dir, "fit2csv.log")
        fmt = "{time} | {level: <8} | {name: ^15} | {function: ^15} | {line: >3} | {message}"
        logger.add(logfile_path, format=fmt)

    def _get_default_temp_dir(self) -> Path:
        home = Path.home()
        assert home is not None, NO_HOME_ASSERT_MSG
        default_temp_dir_path = Path(home, "fitvis-tmp")
        if not default_temp_dir_path.exists():
            default_temp_dir_path.mkdir()
        local_temp_dir_path = default_temp_dir_path
        return local_temp_dir_path

    def _init_csv_path(self) -> Path:
        assert self.fit_path.is_file(), f"No fit file found at {self.fit_path}"
        ino = self.fit_path.stat().st_ino
        filename = f"{self.fit_path.stem}-{ino}.csv"
        return Path(self._temp_dir_path, filename)

    def _clean_csv_file(self) -> None:
        self.csv_path.unlink(missing_ok=True)

    def _process_fit2csv(self):
        success = False
        if not self.csv_path.exists() or self._reprocess:

            if self._reprocess:
                logger.info(f"Removing {self.csv_path} for reprocessing")
                self._clean_csv_file()

            fit2csv_cmd = subprocess.run(
                args=[
                    "java",
                    "-jar",
                    f"{FIT_SDK_PATH}",
                    "-b",
                    f"{self.fit_path}",
                    f"{self.csv_path}",
                ],
                capture_output=True,
            )

            logger.info(fit2csv_cmd.stdout.decode("utf-8"))

            if fit2csv_cmd.returncode == 0:
                success = True
            else:
                logger.error(f"Failed to Process {self.fit_path} to csv")
                logger.error(fit2csv_cmd.stderr.decode("utf-8"))
        else:
            logger.info(
                f"Found {self.csv_path}. Set reprocess to True if reprocessing is required"
            )
            success = True

        return success

    @staticmethod
    def _get_fit_field_structs(df):
        def get_field_param(col_name) -> Optional[Tuple[int, str]]:
            split = col_name.split()
            if len(split) == 2 and split[0] in FIELD_ATTRS:
                return int(split[1]), F.col(col_name).alias(split[0].lower())
            else:
                return None

        field_params: Dict[int, List] = defaultdict(list)
        for col in df.columns:
            if col not in RECORD_ATTRS:
                field_param = get_field_param(col)
                if field_param is not None:
                    field_params[field_param[0]].append(field_param[1])

        field_structs = []
        for array in field_params.values():
            field_structs.append(F.struct(array))
        return field_structs

    @staticmethod
    def bit_any(*conditionals) -> Column:
        conditional = conditionals[0]
        for cond in conditionals[1:]:
            conditional = conditional | cond
        return conditional

    def _init_csv_df(self) -> DataFrame:
        NULL_FIELD_EXCLUDE_ARRAY = F.array(
            F.struct(
                F.lit("").alias("field"),
                F.lit("").alias("value"),
                F.lit("").alias("units"),
            ),
            F.struct(
                F.lit(None).alias("field"),
                F.lit(None).alias("value"),
                F.lit(None).alias("units"),
            ),
        )

        assert self._process_fit2csv()

        base_df = (
            self._spark.read.csv(f"{self.csv_path}", header=True)
            .withColumnRenamed("Type", "record_type")
            .where(F.col("record_type") == "Data")
        )
        all_fields = F.array(self._get_fit_field_structs(base_df)).alias("all_fields")
        fields = F.array_except(all_fields, NULL_FIELD_EXCLUDE_ARRAY).alias("fields")
        return base_df.select(
            *[F.col(col).alias(col.lower()) for col in RECORD_ATTRS], fields
        )

    def _expode_field_arrays(self) -> DataFrame:

        timestamp_conditionals = [
            F.col("field.field") == "timestamp",
            F.col("field.field") == "time_created",
        ]

        ts_case = F.when(
            self.bit_any(*timestamp_conditionals),
            (F.col("field.value").cast("long") + 631065600).cast("timestamp"),
        ).otherwise(None)

        ts_col = (
            F.first(ts_case, ignorenulls=True)
            .over(Window.partitionBy("message_id"))
            .alias("timestamp")
        )

        field_col = (
            F.when(F.col("field") == "type", F.col("message"))
            .otherwise(F.col("field"))
            .alias("field")
        )
        self._spark.conf.set("spark.sql.shuffle.partitions", "8")
        return (
            self.csv_df.select(
                "*",
                F.explode("fields").alias("field"),
                F.hash("fields").alias("message_id"),
            )
            .drop("fields")
            .select(
                "record_type",
                "local number",
                "message",
                "message_id",
                ts_col,
                "field.*",
            )
            .replace("file_id", "file", "message")
            .select(
                "record_type",
                "local number",
                "message",
                "message_id",
                "timestamp",
                field_col,
                "value",
                "units",
            )
            .join(PROFILE, on=["field", "value"], how="left")
            .select(
                "record_type",
                "local number",
                "message",
                "message_id",
                "timestamp",
                "field",
                F.coalesce("enum_value", "value").alias("value"),
                "units",
            )
        )

    def _expand_message_records(self, message: str) -> DataFrame:
        message_df = (
            self.csv_df.where(F.col("message") == message)
            .select(
                "record_type",
                "message",
                F.hash("fields").alias("record_id"),
                F.explode("fields").alias("field_struct"),
            )
            .select("*", "field_struct.*")
            .drop("field_struct")
            .groupBy("record_type", "message", "record_id")
            .pivot("field")
            .agg(F.max(F.concat_ws("-", F.col("value"), F.col("units"))))
        )

        if "type" in message_df.columns:
            message_type = message.replace("_id", "")
            message_df = message_df.withColumnRenamed("type", message_type)

        enum_cols = set(message_df.columns).intersection(set(PROFILE.keys()))

        for col in enum_cols:
            message_df = message_df.replace(**PROFILE[col], subset=col)
        return message_df

    def _get_message_dataframes(self) -> Tuple:
        message_types: List[str] = (
            self.csv_df.select("message")
            .distinct()
            .rdd.map(lambda row: row.message)
            .collect()
        )

        MesageTypes = namedtuple("MesageTypes", message_types)

        return MesageTypes(
            *[self._expand_message_records(message) for message in message_types]
        )
