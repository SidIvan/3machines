CREATE KEYSPACE IF NOT EXISTS dev WITH REPLICATION = {'class':'NetworkTopologyStrategy','SOCRATES':'2'};

USE dev;

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

CREATE TABLE IF NOT EXISTS deltas_keys (
    symbol ascii,
    hour bigint,
    PRIMARY KEY ((symbol, hour))
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

CREATE TABLE IF NOT EXISTS snapshots_keys (
    symbol ascii,
    hour bigint,
    PRIMARY KEY ((symbol, hour))
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
    PRIMARY KEY ((symbol, hour), update_id)
);

CREATE TABLE IF NOT EXISTS book_ticks_keys (
    symbol ascii,
    hour bigint,
    PRIMARY KEY ((symbol, hour))
);

CREATE TABLE IF NOT EXISTS exchange_info (
    day bigint,
    timestamp_ms bigint,
    ex_info_hash bigint,
    ex_info text,
    PRIMARY KEY (day, timestamp_ms)
);


CREATE TABLE IF NOT EXISTS usd_deltas (
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

CREATE TABLE IF NOT EXISTS usd_deltas_keys (
    symbol ascii,
    hour bigint,
    PRIMARY KEY ((symbol, hour))
);

CREATE TABLE IF NOT EXISTS usd_snapshots (
    symbol ascii,
    timestamp_ms bigint,
    hour bigint,
    type boolean,
    price ascii,
    count ascii,
    last_update_id bigint,
    PRIMARY KEY ((symbol, hour), timestamp_ms, type, price)
);

CREATE TABLE IF NOT EXISTS usd_snapshots_keys (
    symbol ascii,
    hour bigint,
    PRIMARY KEY ((symbol, hour))
);

CREATE TABLE IF NOT EXISTS usd_book_ticks (
    symbol ascii,
    timestamp_ms bigint,
    hour bigint,
    update_id bigint,
    bid_price ascii,
    bid_quantity ascii,
    ask_price ascii,
    ask_quantity ascii,
    PRIMARY KEY ((symbol, hour), update_id)
);

CREATE TABLE IF NOT EXISTS usd_book_ticks_keys (
    symbol ascii,
    hour bigint,
    PRIMARY KEY ((symbol, hour))
);

CREATE TABLE IF NOT EXISTS usd_exchange_info (
    day bigint,
    timestamp_ms bigint,
    ex_info_hash bigint,
    ex_info text,
    PRIMARY KEY (day, timestamp_ms)
);

CREATE TABLE IF NOT EXISTS coin_deltas (
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

CREATE TABLE IF NOT EXISTS coin_deltas_keys (
    symbol ascii,
    hour bigint,
    PRIMARY KEY ((symbol, hour))
);

CREATE TABLE IF NOT EXISTS coin_snapshots (
    symbol ascii,
    timestamp_ms bigint,
    hour bigint,
    type boolean,
    price ascii,
    count ascii,
    last_update_id bigint,
    PRIMARY KEY ((symbol, hour), timestamp_ms, type, price)
);

CREATE TABLE IF NOT EXISTS coin_snapshots_keys (
    symbol ascii,
    hour bigint,
    PRIMARY KEY ((symbol, hour))
);

CREATE TABLE IF NOT EXISTS coin_book_ticks (
    symbol ascii,
    timestamp_ms bigint,
    hour bigint,
    update_id bigint,
    bid_price ascii,
    bid_quantity ascii,
    ask_price ascii,
    ask_quantity ascii,
    PRIMARY KEY ((symbol, hour), update_id)
);

CREATE TABLE IF NOT EXISTS coin_book_ticks_keys (
    symbol ascii,
    hour bigint,
    PRIMARY KEY ((symbol, hour))
);

CREATE TABLE IF NOT EXISTS coin_exchange_info (
    day bigint,
    timestamp_ms bigint,
    ex_info_hash bigint,
    ex_info text,
    PRIMARY KEY (day, timestamp_ms)
);
