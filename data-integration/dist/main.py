import importlib
import argparse
from datetime import date
from pyspark.sql import SparkSession
import json


def _parse_arguments():
    """ Parse arguments for job Spark submission"""
    parser = argparse.ArgumentParser(description='DWH Pipeline PySpark Args')
    parser.add_argument('--job_name', type=str, required=True, dest='job_name',
                        help='The name of the pyspark job')

    parser.add_argument('--app_name', type=str, required=True, dest='app_name',
                        help='The name of the pyspark application')

    return parser.parse_args()


def main():
    """ Main Function for Spark Jobs """

    args = _parse_arguments()
    job_module = importlib.import_module('jobs.%s' % args.job_name)

    checkpoint = str(date.today())
    print(f'Starting Pipeline for checkpoint {checkpoint} for job {args.job_name}')

    # Create Spark Session

    spark = SparkSession.builder.appName(args.app_name).config('spark.jars.packages',"org.postgresql:postgresql:42.6.0").getOrCreate()
    #
    # spark.sparkContext.addPyFile('jobs.zip')
    # spark.sparkContext.addPyFile('lib.zip')


    # Run SPARK SESSION
    job_module.run_job(spark, args.job_name, checkpoint)


if __name__ == '__main__':
    main()
