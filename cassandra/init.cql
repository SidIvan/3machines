CREATE KEYSPACE IF NOT EXISTS binance_data WITH REPLICATION = {'class':'NetworkTopologyStrategy','3266281-df55181':'2'};

USE binance_data;

CREATE TABLE IF NOT EXISTS deltas (
    symbol ascii,
    hour bigint,
    timestamp_ms bigint,
    type boolean,
    price ascii,
    count ascii,
    first_update_id bigint,
    update_id bigint,
    PRIMARY KEY ((symbol, hour), timestamp_ms, type, price)
);

CREATE TABLE IF NOT EXISTS snapshots (
    symbol ascii,
    timestamp_ms bigint,
    hour bigint,
    type boolean,
    price ascii,
    count ascii,
    last_update_id bigint,
    PRIMARY KEY ((symbol, hour), timestamp_ms, type, price)
);

CREATE TABLE IF NOT EXISTS book_ticks (
    symbol ascii,
    timestamp_ms bigint,
    hour bigint,
    update_id bigint,
    bid_price ascii,
    bid_quantity ascii,
    ask_price ascii,
    ask_quantity ascii,
    PRIMARY KEY ((symbol, hour), timestamp_ms, update_id)
);

CREATE TABLE IF NOT EXISTS exchange_info (
    day bigint,
    timestamp_ms bigint,
    ex_info_hash bigint,
    ex_info text,
    PRIMARY KEY (day, timestamp_ms)
);
