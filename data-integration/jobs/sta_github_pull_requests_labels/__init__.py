import lib.spark as sp
from pyspark.sql import SparkSession, DataFrame, Window

from lib.udfs import extract_repo_id_from_full_url_udf
import pyspark.sql.functions as F
from lib.utils import get_table_name

def _extract_data(spark: SparkSession, job_name: str, checkpoint: str) -> DataFrame:
    raw_df = sp.read_json(spark, checkpoint, job_key=job_name)
    return raw_df


def _transform(raw_df: DataFrame) -> DataFrame:
    df = raw_df \
        .withColumn("lb", F.explode('labels')) \
        .transform(extract_repo_id_from_full_url_udf) \
        .select(
        F.col("id").alias("pull_requests_id"),
        F.col("number").alias("pull_request_number"),
        F.col("repo_name").alias("repo_name"),
        F.col("lb.id").alias('label_id'),
        F.col("lb.name").alias('label_name'),
        F.col("lb.default").alias('label_default')
    )
    return df


def _load_data(df: DataFrame, table_name):
    sp.write_data_frame(df, table_name, mode='overwrite')


# Implement SCD TYPE 1 Overwrite Each file based on the DATE partition
def run_job(spark: SparkSession, job_name: str, checkpoint: str):
    table_name = get_table_name(job_name=job_name, checkpoint=checkpoint)
    _load_data(
        _transform(
            _extract_data(spark, job_name, checkpoint)
        ),
        table_name
    )
