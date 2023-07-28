import os
from pyspark.sql import DataFrame
import pyspark.sql.types as T
import pyspark.sql.functions as F
from jobs.base import BaseSparkJob
from schemas.structs import realtime_transaction_schema
from schemas.column_enums import (
    fraud_transaction_columns,
    non_fraud_transaction_columns,
)
from utils.udfs import get_haversine_distance_udf
from utils.ml import load_feature_pipeline, load_random_forest_model
from functools import partial


class RealtimeInference(BaseSparkJob):
    def setup(self):
        super().setup()

        self.kafka_opts = {
            "kafka.bootstrap.servers": self.config.kafka.bootstrap_server,
            "startingOffsets": self.config.kafka.starting_offset,
            "subscribe": self.config.kafka.topic,
        }
        spark_opts = {
            "spark.cassandra.connection.host": self.config.spark.cassandra_host,
            "keyspace": self.config.cassandra.keyspace,
        }
        self.spark_opts_customer_table = {
            **spark_opts,
            "table": self.config.cassandra.table_customer,
        }
        self.spark_opts_fraud_table = {
            **spark_opts,
            "table": self.config.cassandra.table_fraud,
        }
        self.spark_opts_non_fraud_table = {
            **spark_opts,
            "table": self.config.cassandra.table_non_fraud,
        }

        self.path_feature_pipeline = os.path.join(
            self.config.s3.path_ml_artifacts, self.config.run_id, "feature_pipeline.out"
        )
        self.path_model = os.path.join(
            self.config.s3.path_ml_artifacts, self.config.run_id, "model.out"
        )

    def load_from_cassandra(self, options):
        return (
            self.spark.read.format(self.config.spark.cassandra_format)
            .options(**options)
            .load()
        )

    def read_from_kafka(self):
        return self.spark.readStream.format("kafka").options(**self.kafka_opts).load()

    def run(self):
        super().run()
        customer_df = self.load_from_cassandra(self.spark_opts_customer_table)
        customer_age_df = customer_df.withColumn(
            "age",
            (F.datediff(F.current_date(), F.to_date("dob")) / 365).cast(
                T.IntegerType()
            ),
        )
        customer_age_df.cache()
        print("Customer age df done.")
        customer_age_df.show(n=3)

        transactions_df = self.read_from_kafka()
        transactions_df = (
            transactions_df.withColumn(
                "transformed",
                F.from_json(
                    F.col("value").cast(T.StringType()), realtime_transaction_schema
                ),
            )
            .selectExpr("transformed.*")
            .withColumn("amt", F.col("amt").cast(T.DoubleType()))
            .withColumn("merch_lat", F.col("merch_lat").cast(T.DoubleType()))
            .withColumn("merch_long", F.col("merch_long").cast(T.DoubleType()))
            .drop("first")
            .drop("last")
        )

        print("Read from stream.")

        processed_df = (
            transactions_df.join(
                F.broadcast(customer_age_df).alias("cust"), "cc_num", how="inner"
            )
            .withColumn(
                "distance",
                F.round(
                    get_haversine_distance_udf(
                        "cust.lat", "cust.long", "merch_lat", "merch_long"
                    ),
                    2,
                ),
            )
            .select(
                "cc_num",
                "trans_num",
                F.to_timestamp("trans_time").alias("trans_time"),
                "category",
                "merchant",
                "amt",
                "merch_lat",
                "merch_long",
                "distance",
                "age",
            )
        )

        print("Done processing.")

        processing_model = load_feature_pipeline(self.path_feature_pipeline)
        features_df = processing_model.transform(processed_df)
        print("Feature transformation done.")

        model = load_random_forest_model(self.path_model)
        predictions_df = model.transform(features_df).withColumnRenamed(
            "prediction", "is_fraud"
        )
        print("Model inference done.")

        fraud_predictions_df = predictions_df.filter(F.col("is_fraud") == 1.0).select(
            fraud_transaction_columns
        )
        non_fraud_predictions_df = predictions_df.filter(
            F.col("is_fraud") != 1.0
        ).select(non_fraud_transaction_columns)

        fraud_batch_writer = partial(
            RealtimeInference.writer,
            self.config.spark.cassandra_format,
            self.spark_opts_fraud_table,
        )
        non_fraud_batch_writer = partial(
            RealtimeInference.writer,
            self.config.spark.cassandra_format,
            self.spark_opts_non_fraud_table,
        )
        fraud_predictions_df.printSchema()
        self.save_for_each(
            fraud_predictions_df, fraud_batch_writer, self.spark_opts_fraud_table
        )
        self.save_for_each(
            non_fraud_predictions_df,
            non_fraud_batch_writer,
            self.spark_opts_non_fraud_table,
        )

    def writeStreamData(self, dataFrame: DataFrame):
        (
            dataFrame.writeStream.format("console")
            .outputMode("append")
            .start()
            .awaitTermination()
        )

    @staticmethod
    def writer(format, options, batch_df: DataFrame, _):
        (batch_df.write.format(format).mode("append").options(**options).save())

    def save_for_each(self, df: DataFrame, batchwriter, options, mode: str = "update"):
        print(options)
        (
            df.writeStream.options(**options)
            .foreachBatch(batchwriter)
            .outputMode(mode)
            .start()
            .awaitTermination()
        )


def run(spark, config):
    job = RealtimeInference(spark, config)
    job.setup()
    job.run()