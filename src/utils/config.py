import typing as t
from dataclasses import dataclass


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

    @classmethod
    def from_dict(cls: t.Type["S3Config"], obj: dict):
        return cls(
            path_transactions=obj["path_transactions"],
            path_customer=obj["path_customer"],
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
class Config:
    spark: SparkConfig
    cassandra: CassandraConfig
    s3: S3Config

    @classmethod
    def from_dict(cls: t.Type["Config"], obj: dict):
        return cls(
            spark=SparkConfig.from_dict(obj["spark"]),
            cassandra=CassandraConfig.from_dict(obj["cassandra"]),
            s3=S3Config.from_dict(obj["s3"]),
        )
