import lib.spark as sp
from pyspark.sql import SparkSession, DataFrame, Window
from lib.utils import get_config_value
import pyspark.sql.functions as F
from lib.utils import get_table_name


def _extract_data(spark: SparkSession, job_name: str):
    src_issues = get_config_value('JOBS', job_name)['SRC_TABLE']
    src_pull_requests = get_config_value('JOBS', job_name)['SRC_TABLE_2']

    df_issues: DataFrame = sp.read_data_frame_from_jdbc(spark, src_issues)
    df_pull_requests: DataFrame = sp.read_data_frame_from_jdbc(spark, src_pull_requests)

    return df_issues, df_pull_requests


def _transform(raw_df_issue: DataFrame, raw_df_pull_requests: DataFrame) -> DataFrame:

    df = raw_df_pull_requests.alias('a'). \
        join(raw_df_issue.alias('b'), raw_df_pull_requests.number == raw_df_issue.pull_request_number, how='left') \
        .select(F.col('a.id').alias('pull_requests_id'),
                F.col('a.title').alias('pull_request_title'),
                F.col('a.created_at').alias('pull_request_created_at'),
                F.col('b.id').alias('issue_id'),
                F.col('b.number').alias('issue_number'),
                F.col('a.number').alias('pull_request_number'),
                F.col('b.created_at').alias('issue_created_at'),
                F.col('a.merged_at').alias('pull_request_merged_at'),
                F.col('a.state').alias('pull_request_state'),
                F.col('b.state').alias('issue_state'),
                F.col('a.milestone_number').alias('milestone_number'),
                F.col('a.milestone_title').alias('milestone_title'),
                F.col('a.additions').alias('lines_additions'),
                F.col('a.deletions').alias('lines_deletions'),
                (F.col('a.additions') + F.col('a.deletions')).alias('total_lines_changed'),
                F.round(((F.col('a.closed_at').cast('long') - F.col('a.created_at').cast('long')) / 60), 2).alias(
                    'pull_request_lead_time_mins'),
                F.round(((F.col('a.merged_at').cast('long') - F.col('b.created_at').cast('long')) / 60), 2).alias(
                    'merge_interval'),
                F.col('a.updated_at').alias('updated_at')
                )

    # Drop duplicates and Keep the latest data
    window = Window.partitionBy("pull_requests_id").orderBy(F.desc("updated_at"))
    df_removed_duplicates = df.select('*', F.row_number().over(window).alias('position')).where('position==1').drop(
        'position')

    return df_removed_duplicates


def _load_data(df: DataFrame, table_name):
    sp.write_data_frame(df, table_name, mode='overwrite')


# Implement Overwrite existing table with new calculated aggregation
def run_job(spark: SparkSession, job_name: str, checkpoint: str):
    table_name = get_table_name(job_name=job_name, checkpoint=checkpoint)
    df_issues, df_pull_requests = _extract_data(spark, job_name)
    _load_data(
        _transform(df_issues, df_pull_requests),
        table_name
    )
