import typing as t
from dataclasses import dataclass
from uuid import uuid4


@dataclass
class MLConfig:
    feature_cols: list
    train_pct: float
    seed: int
    feature_col_name: str
    label_col_name: str
    kmeans_extra_args: dict
    model_extra_args: dict

    @classmethod
    def from_dict(cls: t.Type["MLConfig"], obj: dict):
        return cls(
            feature_cols=obj["feature_cols"],
            train_pct=obj.get("train_pct", 0.7),
            seed=obj.get("seed", 123),
            feature_col_name=obj.get("feature_col_name", "features"),
            label_col_name=obj.get("label_col_name", "label"),
            kmeans_extra_args=obj.get("kmeans_extra_args", {}),
            model_extra_args=obj.get("model_extra_args", {}),
        )


@dataclass
class SparkConfig:
    app_name: str
    master: str
    jars: str
    exec_mem: str
    cassandra_format: str
    cassandra_host: str

    @classmethod
    def from_dict(cls: t.Type["SparkConfig"], obj: dict):
        return cls(
            app_name=obj["app_name"],
            master=obj["master"],
            jars=",".join(obj.get("jars", [])),
            exec_mem=obj.get("exec_mem", "480m"),
            cassandra_host=obj["cassandra_host"],
            cassandra_format=obj.get(
                "cassandra_format", "org.apache.spark.sql.cassandra"
            ),
        )


@dataclass
class S3Config:
    path_transactions: str
    path_customer: str
    path_ml_artifacts: str

    @classmethod
    def from_dict(cls: t.Type["S3Config"], obj: dict):
        return cls(
            path_transactions=obj["path_transactions"],
            path_customer=obj["path_customer"],
            path_ml_artifacts=obj["path_ml_artifacts"],
        )


@dataclass
class CassandraConfig:
    keyspace: str
    table_customer: str
    table_fraud: str
    table_non_fraud: str

    @classmethod
    def from_dict(cls: t.Type["CassandraConfig"], obj: dict):
        return cls(
            keyspace=obj["keyspace"],
            table_customer=obj["table_customer"],
            table_fraud=obj["table_fraud"],
            table_non_fraud=obj["table_non_fraud"],
        )


@dataclass
class KafkaConfig:
    bootstrap_server: str
    starting_offset: str
    topic: str

    @classmethod
    def from_dict(cls: t.Type["KafkaConfig"], obj: dict):
        return cls(
            bootstrap_server=obj["bootstrap_server"],
            starting_offset=obj.get("starting_offset", "earliest"),
            topic=obj["topic"],
        )


@dataclass
class Config:
    spark: SparkConfig
    cassandra: CassandraConfig
    kafka: KafkaConfig
    s3: S3Config
    ml: MLConfig
    run_id: str

    @classmethod
    def from_dict(cls: t.Type["Config"], obj: dict):
        return cls(
            run_id=obj.get("run_id", uuid4().hex),
            spark=SparkConfig.from_dict(obj["spark"]),
            cassandra=CassandraConfig.from_dict(obj["cassandra"]),
            kafka=KafkaConfig.from_dict(obj["kafka"]),
            s3=S3Config.from_dict(obj["s3"]),
            ml=MLConfig.from_dict(obj["ml"]),
        )
