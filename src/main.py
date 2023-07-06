import argparse
import importlib

from pyspark.sql import SparkSession
from utils.config_parser import ConfigParser


def parse_opt():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, help="job name to run")
    parser.add_argument(
        "--config", default="configs/config.json", help="config file the job should use"
    )
    opt = parser.parse_args()
    return opt


def parse_config(config_file):
    cfg_parser = ConfigParser(config_file)
    return cfg_parser.parse_config()


def init_or_get_spark(config, spark):
    if spark is None:
        spark = SparkSession.builder.appName(config.get("app_name")).getOrCreate()
    return spark


def main():
    opt = parse_opt()
    config = parse_config(opt.config)
    spark = init_or_get_spark(config)

    job_module = importlib.import_module(f"jobs.{opt.job}.entrypoint")
    getattr(job_module, "run")(spark, config)


if __name__ == "__main__":
    main()
