#!/bin/bash
. "/opt/spark/bin/load-spark-env.sh"
# When the spark work_load is master run class org.apache.spark.deploy.master.Master
if [ "$SPARK_WORKLOAD" == "master" ]; then
	SPARK_MASTER_HOST=$(hostname)
	export SPARK_MASTER_HOST
	cd /opt/spark/bin && ./spark-class org.apache.spark.deploy.master.Master --ip "$SPARK_MASTER_HOST" --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_MASTER_WEBUI_PORT" >>"$SPARK_MASTER_LOG"

elif [ "$SPARK_WORKLOAD" == "worker" ]; then
	# When the spark work_load is worker run class org.apache.spark.deploy.master.Worker
	cd /opt/spark/bin && ./spark-class org.apache.spark.deploy.worker.Worker --webui-port "$SPARK_WORKER_WEBUI_PORT" "$SPARK_MASTER" >>"$SPARK_WORKER_LOG"

elif [ "$SPARK_WORKLOAD" == "submit" ]; then
	echo "----- Submitting Job -----"
	cd /opt/spark-apps/dist || return
	spark-submit --master "$SPARK_MASTER" \
		--py-files jobs.zip,shared.zip \
		--files config.json,etl_config.json main.py \
		--job dota_parse_raw \
		--config etl_config.json
else
	echo "Undefined Workload Type $SPARK_WORKLOAD, must specify: master, worker, submit"
fi
