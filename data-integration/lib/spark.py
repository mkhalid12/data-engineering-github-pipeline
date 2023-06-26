from pyspark.sql import SparkSession, DataFrame
from lib.utils import get_config_value
from datetime import date, timedelta

def get_spark_session() -> SparkSession:
    SPARK_HOST = get_config_value('SPARK', 'HOST')
    APP_NAME = get_config_value('SPARK', 'APP_NAME')
    RESOURCE_JAR = get_config_value('SPARK', 'PG_JAR_PATH')
    spark = (SparkSession.builder
            .master(SPARK_HOST)
            .appName(APP_NAME)
            .config('spark.driver.extraClassPath', RESOURCE_JAR)
            .getOrCreate())
    spark.sparkContext.addFile('')
    return spark

def read_json(spark: SparkSession, checkpoint: str, job_key: str) -> DataFrame:
    data_dir = get_config_value('CORE', 'DATA_DIR_PATH')
    file_extension = get_config_value('CORE', 'DATA_FILE_EXT')
    job_name = get_config_value('JOBS', job_key).get('FILE_PATH')
    json_file = f"{data_dir}/{job_name}/{checkpoint}/{file_extension}"
    print('--->', json_file)
    df = spark.read.json(json_file, multiLine=True)
    return df


def write_data_frame(df: DataFrame, table: str, mode: str):
    properties = get_config_value('SINK', 'DWH')
    url = properties['url']
    df.write.option('driver', 'org.postgresql.Driver').option("truncate", 'true').jdbc(url, table, mode, properties)


def read_data_frame_from_jdbc(spark: SparkSession, table: str) -> DataFrame:
    properties = get_config_value('SINK', 'DWH')
    url = properties['url']
    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option('driver', 'org.postgresql.Driver') \
        .option("user", properties['user']) \
        .option("password", properties['password']) \
        .option("dbtable", table).load()
    return df

