import os
import csv
import json
import boto3
from datetime import datetime
from urllib.parse import urlparse
from time import sleep
from kafka import KafkaProducer


def get_s3_resource(**kwargs):
    return boto3.resource(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        **kwargs,
    )


def read_from_s3(s3_obj, bucket, key):
    obj = s3_obj.Object(bucket, key)
    data = obj.get()["Body"].read().decode()
    reader = csv.reader(
        data.splitlines()[1:],
        delimiter=",",
        quotechar='"',
        quoting=csv.QUOTE_ALL,
        skipinitialspace=True,
    )
    for record in reader:
        yield record


def on_send_success(record_metadata):
    print(record_metadata)
    print(record_metadata.topic, record_metadata.partition, record_metadata.offset)


def on_send_error(excp):
    print("I am an errback", exc_info=excp)


def rec2json(record):
    now = str(datetime.now())
    data = {
        "cc_num": record[0],
        "first": record[1],
        "last": record[2],
        "trans_num": record[3],
        "trans_time": now,
        "category": record[7],
        "merchant": record[8],
        "amt": record[9],
        "merch_lat": record[10],
        "merch_long": record[11],
    }
    return record[-1], data


if __name__ == "__main__":
    path_dataset = os.environ["DATASET_TRANSACTIONS"]
    path_parsed = urlparse(path_dataset, allow_fragments=False)
    bucket, key = path_parsed.netloc, path_parsed.path.lstrip("/")

    s3_obj = get_s3_resource(
        aws_session_token=None,
        config=boto3.session.Config(signature_version="s3v4"),
        endpoint_url="http://localhost:9000",
        verify=False,
    )

    producer = KafkaProducer(
        bootstrap_servers=["localhost:19092"],
        value_serializer=lambda m: json.dumps(m).encode("ascii"),
    )

    maxlines = 10
    for record in read_from_s3(s3_obj, bucket, key):
        is_fraud, json_payload = rec2json(record)
        if is_fraud == "1":
            producer.send(bucket + "_v4", json_payload).add_callback(
                on_send_success
            ).add_errback(on_send_error)
            sleep(6)
            maxlines -= 1
            if maxlines <= 0:
                break
    producer.flush()
