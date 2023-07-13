from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)
from schemas.column_enums import Customer, Transaction

customer_schema = StructType(
    [
        StructField(Customer.cc_num.value, StringType(), True),
        StructField(Customer.first.value, StringType(), True),
        StructField(Customer.last.value, StringType(), True),
        StructField(Customer.gender.value, StringType(), True),
        StructField(Customer.street.value, StringType(), True),
        StructField(Customer.city.value, StringType(), True),
        StructField(Customer.state.value, StringType(), True),
        StructField(Customer.zip.value, StringType(), True),
        StructField(Customer.lat.value, DoubleType(), True),
        StructField(Customer.long.value, DoubleType(), True),
        StructField(Customer.job.value, StringType(), True),
        StructField(Customer.dob.value, TimestampType(), True),
    ]
)

transaction_schema = StructType(
    [
        StructField(Transaction.cc_num.value, StringType(), True),
        StructField(Transaction.first.value, StringType(), True),
        StructField(Transaction.last.value, StringType(), True),
        StructField(Transaction.trans_num.value, StringType(), True),
        StructField(Transaction.trans_date.value, StringType(), True),
        StructField(Transaction.trans_time.value, StringType(), True),
        StructField(Transaction.unix_time.value, LongType(), True),
        StructField(Transaction.category.value, StringType(), True),
        StructField(Transaction.merchant.value, StringType(), True),
        StructField(Transaction.amt.value, DoubleType(), True),
        StructField(Transaction.merch_lat.value, DoubleType(), True),
        StructField(Transaction.merch_long.value, DoubleType(), True),
    ]
)

fraud_transaction_schema = transaction_schema.add(
    StructField(Transaction.is_fraud.value, DoubleType(), True)
)

realtime_transaction_schema = StructType(
    [
        StructField(Transaction.cc_num.value, StringType(), True),
        StructField(Transaction.first.value, StringType(), True),
        StructField(Transaction.last.value, StringType(), True),
        StructField(Transaction.trans_num.value, StringType(), True),
        StructField(Transaction.trans_time.value, TimestampType(), True),
        StructField(Transaction.category.value, StringType(), True),
        StructField(Transaction.merchant.value, StringType(), True),
        StructField(Transaction.amt.value, StringType(), True),
        StructField(Transaction.merch_lat.value, StringType(), True),
        StructField(Transaction.merch_long.value, StringType(), True),
    ]
)
