import pyspark.sql.types as T
from pyspark.context import SparkContext
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.linalg import Vectors


def evaluate_classifier_predictions(
    predictions, label_col="label", pred_col="prediction"
):
    evaluator = MulticlassClassificationEvaluator(
        labelCol=label_col, predictionCol=pred_col
    )
    accuracy = evaluator.evaluate(predictions)
    return accuracy


def run_random_forest_classifier(
    train_df: DataFrame,
    test_df: DataFrame,
    label_col="label",
    features_col="features",
    **kwargs,
):
    model_extra_args = kwargs.get("model_extra_args", {})
    print(model_extra_args)
    rf = RandomForestClassifier(
        featuresCol=features_col, labelCol=label_col, **model_extra_args
    )
    model = rf.fit(train_df)
    predictions = model.transform(test_df)

    metric = evaluate_classifier_predictions(
        predictions,
    )

    return model, metric


def create_feature_pipeline(
    schema: T.StructType, columns: list, features_col="features"
):
    categorical_cols = [
        f.name
        for f in schema.fields
        if isinstance(f.dataType, T.StringType) and f.name in columns
    ]
    numerical_cols = [
        f.name
        for f in schema.fields
        if isinstance(f.dataType, T.NumericType) and f.name in columns
    ]

    stage_string = [
        StringIndexer(inputCol=c, outputCol=c + "_string_encoded", handleInvalid="keep")
        for c in categorical_cols
    ]
    stage_one_hot = [
        OneHotEncoder(inputCol=c + "_string_encoded", outputCol=c + "_one_hot")
        for c in categorical_cols
    ]

    features = [c + "_string_encoded" for c in categorical_cols] + numerical_cols
    assembler = VectorAssembler(inputCols=features, outputCol=features_col)

    stages = stage_string + stage_one_hot + [assembler]
    return Pipeline(stages=stages)


def kmeans_undersampling(
    df: DataFrame,
    sc: SparkContext,
    target_count,
    features_col="features",
    **kwargs,
):
    kmeans_extra_args = kwargs.get("kmeans_extra_args", {})
    print(kmeans_extra_args)
    kmeans = KMeans(k=target_count, featuresCol=features_col, **kmeans_extra_args)
    model = kmeans.fit(df)
    cluster_centers = model.clusterCenters()
    rdd = sc.parallelize([c.tolist() for c in cluster_centers])
    return rdd.map(lambda x: (Vectors.dense(x),)).toDF([features_col])
