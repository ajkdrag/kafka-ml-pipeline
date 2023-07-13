import os
import json
import boto3
from datetime import datetime
from urllib.parse import urlparse
from kafka import KafkaProducer
from kafka.errors import KafkaError


def get_s3_resource(**kwargs):
    return boto3.resource(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        **kwargs,
    )


def read_from_s3(s3_obj, bucket, key, maxlines=10):
    obj = s3_obj.Object(bucket, key)
    data = obj.get()["Body"].read().decode()
    for idx, line in enumerate(data.splitlines()):
        print(idx)
        yield line
        if idx == maxlines:
            break


def on_send_success(record_metadata):
    print(record_metadata)
    print(record_metadata.topic, record_metadata.partition, record_metadata.offset)


def on_send_error(excp):
    print("I am an errback", exc_info=excp)


def create_record(line):
    record = line.split(",")
    now = str(datetime.now())
    data = {
        "cc_num": record[0],
        "first": record[1],
        "last": record[2],
        "trans_num": record[3],
        "trans_time": now,
        "category": record[6],
        "merchant": record[7],
        "amt": record[8],
        "merch_lat": record[9],
        "merch_long": record[10],
        "distance": record[11],
        "age": record[12],
    }
    print(data)
    return data


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

    for line in read_from_s3(s3_obj, bucket, key):
        json_payload = create_record(line)
        producer.send(bucket, json_payload).add_callback(on_send_success).add_errback(
            on_send_error
        )
    producer.flush()
