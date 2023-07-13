import os
import pyspark.sql.types as T
import pyspark.sql.functions as F
from jobs.base import BaseSparkJob
from utils.ml import (
    create_feature_pipeline,
    kmeans_undersampling,
    run_random_forest_classifier,
)


class FraudDetectionTraining(BaseSparkJob):
    def setup(self):
        super().setup()
        spark_opts = {
            "spark.cassandra.connection.host": self.config.spark.cassandra_host,
            "keyspace": self.config.cassandra.keyspace,
        }
        self.spark_opts_fraud_table = {
            **spark_opts,
            "table": self.config.cassandra.table_fraud,
        }
        self.spark_opts_non_fraud_table = {
            **spark_opts,
            "table": self.config.cassandra.table_non_fraud,
        }
        self.path_feature_pipeline = os.path.join(self.config.s3.path_ml_artifacts, self.config.run_id, "feature_pipeline")
        self.path_model = os.path.join(self.config.s3.path_ml_artifacts, self.config.run_id, "model")

    def load_from_cassandra(self, options):
        return (
            self.spark.read.format(self.config.spark.cassandra_format)
            .options(**options)
            .load()
        )

    def feature_engineering(self, train_df, test_df):
        pipeline = create_feature_pipeline(
            train_df.schema,
            self.config.ml.feature_cols,
            features_col=self.config.ml.feature_col_name,
        )

        # 1. feature transformations
        feature_transfomer = pipeline.fit(train_df)
        train_features_df = feature_transfomer.transform(train_df)
        test_features_df = feature_transfomer.transform(test_df)

        # TODO: save pipeline
        feature_transfomer.save(self.path_feature_pipeline)

        # 2. undersample negative class in train data.
        fraud_train_df = train_features_df.filter(F.col("is_fraud") == 1).select(
            self.config.ml.feature_col_name,
            F.col("is_fraud").alias(self.config.ml.label_col_name),
        )
        non_fraud_train_df = train_features_df.filter(F.col("is_fraud") == 0).select(
            self.config.ml.feature_col_name
        )
        undersampled_non_fraud_train_df = kmeans_undersampling(
            non_fraud_train_df,
            self.spark.sparkContext,
            fraud_train_df.count(),
            features_col=self.config.ml.feature_col_name,
            kmeans_extra_args=self.config.ml.kmeans_extra_args,
        )

        # 3. prepare and return final train and test data
        final_train_df = fraud_train_df.union(
            undersampled_non_fraud_train_df.withColumn(
                self.config.ml.label_col_name, F.lit(0.0)
            )
        )
        final_test_df = test_features_df.select(
            self.config.ml.feature_col_name,
            F.col("is_fraud").alias(self.config.ml.label_col_name),
        )

        return final_train_df, final_test_df

    def run(self):
        super().run()

        fraud_df = self.load_from_cassandra(self.spark_opts_fraud_table)
        non_fraud_df = self.load_from_cassandra(self.spark_opts_non_fraud_table)
        transactions_df = fraud_df.union(non_fraud_df)
        transactions_df.cache()

        # train-test split
        train_df, test_df = transactions_df.randomSplit(
            [self.config.ml.train_pct, 1 - self.config.ml.train_pct],
            seed=self.config.ml.seed,
        )

        # feature engineering
        final_train_df, final_test_df = self.feature_engineering(train_df, test_df)

        # train and evaluate model
        model, metrics = run_random_forest_classifier(
            final_train_df,
            final_test_df,
            label_col=self.config.ml.label_col_name,
            features_col=self.config.ml.feature_col_name,
            model_extra_args=self.config.ml.model_extra_args,
        )

        model.save(self.path_model)
        print(metrics)


def run(spark, config):
    job = FraudDetectionTraining(spark, config)
    job.setup()
    job.run()
