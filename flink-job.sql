-- TODO: Verify and review FIRST_VALUE/LAST_VALUE order guarantee in distributed environment during TUMBLE window aggregation
-- Flink SQL script for real-time cryptocurrency trade aggregation

-- 1. Define Kafka Source Table
CREATE TABLE kafka_trades (
    event_time BIGINT,
    symbol STRING,
    trade_id BIGINT,
    price DOUBLE,
    quantity DOUBLE,
    trade_time BIGINT,
    is_buyer_maker BOOLEAN,
    ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
) WITH (
    'connector' = 'kafka',
    'topic-pattern' = 'crypto-trades',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-aggregator',
    'scan.startup.mode' = 'latest-offset',
    'scan.topic-partition-discovery.interval' = '10s',
    'format' = 'json'
);

-- 2. Define Redis Sink Table
CREATE TABLE redis_volume_sink (
    redis_key STRING,
    redis_field STRING,
    redis_value STRING,
    PRIMARY KEY (redis_key, redis_field) NOT ENFORCED
) WITH (
    'connector' = 'redis',
    'host' = 'redis',
    'port' = '6379',
    'redis-mode' = 'single',
    'command' = 'hset'
);

-- 3. Execute Cumulative Real-time & Window Aggregations
EXECUTE STATEMENT SET BEGIN

-- A. Cumulative Metrics (Global state)
INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'volume' AS redis_field,
    CAST(SUM(quantity) AS STRING) AS redis_value
FROM kafka_trades
GROUP BY symbol;

INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'trade_count' AS redis_field,
    CAST(COUNT(trade_id) AS STRING) AS redis_value
FROM kafka_trades
GROUP BY symbol;

INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'global_vwap' AS redis_field,
    -- Rename to unify meaning under cumulative Volume-Weighted Average Price (VWAP)
    CAST(SUM(price * quantity) / SUM(quantity) AS STRING) AS redis_value
FROM kafka_trades
GROUP BY symbol;

INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'event_time' AS redis_field,
    CAST(MAX(event_time) AS STRING) AS redis_value
FROM kafka_trades
GROUP BY symbol;


-- B. Recent 1-Min Sliding Metrics (HOP Window: 5s slide, 1m size)
INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'vwap_1m' AS redis_field,
    CAST(SUM(price * quantity) / SUM(quantity) AS STRING) AS redis_value
FROM TABLE(
    HOP(TABLE kafka_trades, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;

INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'volume_1m' AS redis_field,
    CAST(SUM(quantity) AS STRING) AS redis_value
FROM TABLE(
    HOP(TABLE kafka_trades, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;

INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'volatility_1m' AS redis_field,
    -- Null protection fallback for idle periods with 1 or fewer transactions
    CAST(COALESCE(STDDEV_SAMP(price), 0.0) AS STRING) AS redis_value
FROM TABLE(
    HOP(TABLE kafka_trades, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;

INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'spread_1m' AS redis_field,
    CAST(MAX(price) - MIN(price) AS STRING) AS redis_value
FROM TABLE(
    HOP(TABLE kafka_trades, DESCRIPTOR(ts), INTERVAL '5' SECOND, INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;


-- C. Latest 1-Min Candlestick Metrics (TUMBLE Window: 1m size)
INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'open_1m' AS redis_field,
    CAST(FIRST_VALUE(price) AS STRING) AS redis_value
FROM TABLE(
    TUMBLE(TABLE kafka_trades, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;

INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'high_1m' AS redis_field,
    CAST(MAX(price) AS STRING) AS redis_value
FROM TABLE(
    TUMBLE(TABLE kafka_trades, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;

INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'low_1m' AS redis_field,
    CAST(MIN(price) AS STRING) AS redis_value
FROM TABLE(
    TUMBLE(TABLE kafka_trades, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;

INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'close_1m' AS redis_field,
    CAST(LAST_VALUE(price) AS STRING) AS redis_value
FROM TABLE(
    TUMBLE(TABLE kafka_trades, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;

INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'volume_1m_tumble' AS redis_field,
    CAST(SUM(quantity) AS STRING) AS redis_value
FROM TABLE(
    TUMBLE(TABLE kafka_trades, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;

INSERT INTO redis_volume_sink
SELECT
    CONCAT(symbol, '_volume_agg') AS redis_key,
    'candle_end_time' AS redis_field,
    -- Store minutely window end time metadata
    CAST(window_end AS STRING) AS redis_value
FROM TABLE(
    TUMBLE(TABLE kafka_trades, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
GROUP BY symbol, window_start, window_end;

END;
