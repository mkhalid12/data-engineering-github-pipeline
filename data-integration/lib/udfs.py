from pyspark.sql import SparkSession, DataFrame

from lib.utils import get_config_value
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from urllib.parse import urlparse


def parse_struct_type_to_json(df: DataFrame) -> DataFrame:
    # find all struck/array columns
    struct_columns = {c: '{}' for c, t in df.dtypes if str(t).startswith('struct') or str(t).startswith('array')}
    # Parse struct type to to_json str
    select_expr = [F.to_json(F.col(c)).alias(c) if c in struct_columns else F.col(c) for c in df.columns]
    df_struck_to_json = df.select(*select_expr)
    return df_struck_to_json.fillna(struct_columns)


def parse_timestamp(df: DataFrame, columns_to_cast: list) -> DataFrame:
    select_expr = [F.to_timestamp(F.col(c), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(c) if c in columns_to_cast else F.col(c)
                   for c in df.columns]
    return df.select(*select_expr)


# Create a function to extract repo from url
def extract_repo_id(df: DataFrame) -> DataFrame:
    return df.selectExpr("*", "replace(parse_url(repository_url, 'PATH'),'/repos/','') as repo_name")

def extract_repo_id_full_url(df: DataFrame) -> DataFrame:
    return df.selectExpr("*",
                         "replace(regexp_extract( parse_url(url, 'PATH') ,'repos/(.*?)/(.*?)/' ,0) ,'repos/') as `repo_name`")


def extract_pull_request(df: DataFrame) -> DataFrame:
    return df.selectExpr("*", 'to_timestamp(pull_request.merged_at) as pull_request_merged_at',
                         "cast (replace(parse_url(pull_request.url, 'PATH'),'/repos/grafana/grafana/pulls/', '')  as bigint) as pull_request_number"
                         )

# Create a function to extract linked issue find in the body
def extract_issue_id_from_body(df: DataFrame) -> DataFrame:
    return df.withColumn('linked_issue_number_in_comments',
                         F.regexp_extract(F.lower(F.col('body')),
                                          '(fix|fixes|fixed|resolve|resolves|resolved|close|closes|closed)\s+#(\d+)',
                                          2).cast('bigint'))



extract_repo_udf = lambda df: extract_repo_id(df)
extract_pull_request_udf = lambda df: extract_pull_request(df)
parse_stuck_to_json_udf = lambda df: parse_struct_type_to_json(df)
cast_timestamp_cols_udf = lambda df: parse_timestamp(df, ['created_at', 'closed_at', 'updated_at'])
cast_timestamp_cols_pull_requests_udf = lambda df: parse_timestamp(df, ['created_at', 'closed_at', 'updated_at', 'merged_at'])

extract_repo_id_from_full_url_udf = lambda df: extract_repo_id_full_url(df)
extract_linked_issue_udf = lambda df: extract_issue_id_from_body(df)

