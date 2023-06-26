import lib.spark as sp
from pyspark.sql import SparkSession, DataFrame
from lib.utils import get_config_value
import pyspark.sql.functions as F
from lib.utils import get_table_name


def _extract_data(spark: SparkSession, job_name: str) -> DataFrame:
    src_table = get_config_value('JOBS', job_name)['SRC_TABLE']
    raw_df = sp.read_data_frame_from_jdbc(spark, src_table)
    return raw_df


def _transform(raw_df: DataFrame) -> DataFrame:
    df = raw_df.groupBy("label_id", 'label_name').agg(
        F.countDistinct("pull_request_number").alias('nb_pull_requests'))
    return df


def _load_data(df: DataFrame, table_name):
    sp.write_data_frame(df, table_name, mode='overwrite')


# Implement Overwrite existing table with new calculated aggregation
def run_job(spark: SparkSession, job_name: str, checkpoint: str):
    table_name = get_table_name(job_name=job_name, checkpoint=checkpoint)
    _load_data(
        _transform(
            _extract_data(spark, job_name)
        ),
        table_name
    )





