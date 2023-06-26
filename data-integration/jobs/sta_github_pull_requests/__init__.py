import lib.spark as sp
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
from lib.utils import get_table_name
from lib.udfs import extract_repo_id_from_full_url_udf, extract_linked_issue_udf, parse_stuck_to_json_udf, cast_timestamp_cols_pull_requests_udf


def _extract_data(spark: SparkSession, job_name: str, checkpoint: str) -> DataFrame:
    raw_df = sp.read_json(spark, checkpoint, job_key=job_name)
    return raw_df


def _transform(raw_df: DataFrame) -> DataFrame:
    # project only required columns
    df_select = raw_df.select("id", "number", "state", "title", "body", "created_at", "updated_at",
                              "closed_at", "url", "merged_at", "labels",
                              F.col("milestone.number").alias('milestone_number'),
                              F.col("milestone.title").alias('milestone_title'),
                              "draft", "merged", "mergeable_state", "additions", "deletions"
                              )

    df_transformed = df_select \
        .transform(extract_linked_issue_udf) \
        .transform(parse_stuck_to_json_udf) \
        .transform(cast_timestamp_cols_pull_requests_udf) \
        .transform(extract_repo_id_from_full_url_udf) \
        .drop('url')

    window = Window.partitionBy("repo_name", "number").orderBy(F.desc("updated_at"))
    df_removed_duplicates = df_transformed.select('*', F.row_number().over(window).alias('position')).where(
        'position==1').drop(
        'position')
    return df_removed_duplicates


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
