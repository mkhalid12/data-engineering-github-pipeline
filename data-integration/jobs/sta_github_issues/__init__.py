import lib.spark as sp
from lib.utils import get_table_name
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
from lib.udfs import extract_repo_udf, extract_pull_request_udf, parse_stuck_to_json_udf, cast_timestamp_cols_udf


def _extract_data(spark: SparkSession, job_name: str, checkpoint: str) -> DataFrame:
    df_issues: DataFrame = sp.read_json(spark, checkpoint, job_key=job_name)

    return df_issues


def _transform(raw_df):
    df_select = raw_df.select("id", "number", "title", "repository_url", "milestone",
                              "pull_request", "labels", "created_at", "state", "state_reason",
                              "closed_at", "updated_at")

    df = df_select \
        .transform(extract_repo_udf) \
        .transform(extract_pull_request_udf) \
        .transform(parse_stuck_to_json_udf) \
        .transform(cast_timestamp_cols_udf) \
        .drop('repository_url') \
        .drop('pull_request')

    # Drop duplicates and Keep the latest data
    window = Window.partitionBy("repo_name", "number").orderBy(F.desc("updated_at"))
    df_removed_duplicates = df.select('*', F.row_number().over(window).alias('position')).where('position==1').drop(
        'position')

    return df_removed_duplicates


def _load_data(df: DataFrame, table_name):
    sp.write_data_frame(df, table_name, mode='overwrite')


# Implement SCD TYPE 1 Overwrite Each file based on the DATE  partition
def run_job(spark: SparkSession, job_name: str, checkpoint: str):
    table_name = get_table_name(job_name=job_name, checkpoint=checkpoint)
    _load_data(
        _transform(
            _extract_data(spark, job_name, checkpoint)
        ),
        table_name
    )
