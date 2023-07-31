CREATE TABLE IF NOT EXISTS creditcard.non_fraud_transaction (
    cc_num text,
    trans_time timestamp,
    age int,
    amt double,
    category text,
    distance double,
    is_fraud double,
    merch_lat double,
    merch_long double,
    merchant text,
    trans_num text,
    PRIMARY KEY (cc_num, trans_time)
) WITH ID = c5f1e690-1b5c-11ee-9c9e-ad8c8961b28a
    AND CLUSTERING ORDER BY (trans_time DESC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND memtable = 'default'
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';
