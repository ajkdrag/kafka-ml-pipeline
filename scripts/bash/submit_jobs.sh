declare -a jars_arr=(
    "s3a://realtime-ml/jars/spark-token-provider-kafka-0-10_2.12-3.0.0.jar"
    "s3a://realtime-ml/jars/kafka-clients-2.4.1.jar"
    "s3a://realtime-ml/jars/spark-cassandra-connector-assembly_2.12-3.0.0.jar"
    "s3a://realtime-ml/jars/spark-sql-kafka-0-10_2.12-3.0.0.jar"
    "s3a://realtime-ml/jars/commons-pool2-2.6.2.jar",
    "s3a://realtime-ml/jars/jnr-posix-3.1.15.jar"
)
jars=$(
    IFS=","
    echo "${jars_arr[*]}"
)
spark-submit --jars "$jars" \
    --master "$SPARK_MASTER" \
    --py-files jobs.zip,schemas.zip,utils.zip \
    --files base_config.json main.py \
    --job import_to_cassandra \
    --config base_config.json
