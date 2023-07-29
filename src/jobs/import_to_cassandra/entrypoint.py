import pyspark.sql.types as T
import pyspark.sql.functions as F
from jobs.base import BaseSparkJob
from schemas.column_enums import non_target_columns, target_column
from schemas.structs import customer_schema, fraud_transaction_schema
from utils.udfs import get_haversine_distance_udf


class ImportToCassandra(BaseSparkJob):
    def setup(self):
        super().setup()
        spark_opts = {
            "spark.cassandra.connection.host": self.config.spark.cassandra_host,
            "keyspace": self.config.cassandra.keyspace,
            "confirm.truncate": True,
        }
        self.spark_opts_fraud_table = {
            **spark_opts,
            "table": self.config.cassandra.table_fraud,
        }
        self.spark_opts_non_fraud_table = {
            **spark_opts,
            "table": self.config.cassandra.table_non_fraud,
        }
        self.spark_opts_customer_table = {
            **spark_opts,
            "table": self.config.cassandra.table_customer,
        }

    def load_customer_df(self):
        customer_df = self.spark.read.csv(
            self.config.s3.path_customer, schema=customer_schema, header=True
        )
        return customer_df

    def load_transactions_df(self):
        transactions_df = (
            self.spark.read.csv(
                self.config.s3.path_transactions,
                schema=fraud_transaction_schema,
                header=True,
            )
            .withColumn("trans_date", F.split("trans_date", "T").getItem(0))
            .withColumn(
                "trans_time",
                F.to_timestamp(F.concat_ws(" ", "trans_date", "trans_time")),
            )
        )
        return transactions_df

    def save_data_to_cassandra(self, df, options, mode="overwrite"):
        (
            df.write.format(self.config.spark.cassandra_format)
            .mode(mode)
            .options(**options)
            .save()
        )

    def run(self):
        super().run()

        # extract
        customer_df = self.load_customer_df()
        transactions_df = self.load_transactions_df()

        # transform
        customer_age_df = customer_df.withColumn(
            "age",
            (F.datediff(F.current_date(), F.to_date("dob")) / 365).cast(
                T.IntegerType()
            ),
        )
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
            .select(*non_target_columns, target_column)
        )
        processed_df.cache()
        fraud_df = processed_df.filter(F.col(target_column) == 1)
        non_fraud_df = processed_df.filter(F.col(target_column) == 0)

        # load
        self.save_data_to_cassandra(customer_df, self.spark_opts_customer_table)
        self.save_data_to_cassandra(fraud_df, self.spark_opts_fraud_table)
        self.save_data_to_cassandra(non_fraud_df, self.spark_opts_non_fraud_table)


def run(spark, config):
    job = ImportToCassandra(spark, config)
    job.setup()
    job.run()
