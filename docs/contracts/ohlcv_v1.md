# OHLCV Data Contract v1

## Overview

This document defines the schema for OHLCV (Open, High, Low, Close, Volume) candlestick data used throughout the trading platform.

## Schema Definition

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `event_time` | timestamp (UTC) | No | Candle open time |
| `symbol` | string | No | Trading pair (e.g., "BTCUSDT") |
| `timeframe` | string | No | Candle interval (e.g., "1h", "1d") |
| `open` | float64 | No | Opening price |
| `high` | float64 | No | Highest price |
| `low` | float64 | No | Lowest price |
| `close` | float64 | No | Closing price |
| `volume` | float64 | No | Trading volume (base asset) |
| `source` | string | No | Data source identifier |
| `ingestion_time` | timestamp (UTC) | No | When data was ingested |

## Validation Rules

1. **Price Consistency**
   - `high >= max(open, close)`
   - `low <= min(open, close)`
   - `high >= low`

2. **Non-negative Values**
   - `volume >= 0`
   - All prices must be >= 0

3. **Required Fields**
   - No null values allowed in any column

## Partitioning Strategy

Data is partitioned hierarchically:

```
data_lake/raw/ohlcv/
└── source={source}/
    └── symbol={symbol}/
        └── timeframe={timeframe}/
            └── year={YYYY}/
                └── month={MM}/
                    └── part-{NNNNN}.parquet
```

### Partition Benefits

- **Query Efficiency**: Filter by source, symbol, timeframe, or date range
- **File Size Control**: Monthly partitions + row limits keep files manageable
- **Parallelization**: Independent partitions can be processed in parallel

## File Format

- **Format**: Apache Parquet
- **Compression**: Snappy
- **Row Ordering**: Ascending by `event_time`
- **Deduplication Key**: `(event_time, timeframe)`

## Schema Version History

| Version | Date | Changes |
|---------|------|---------|
| v1 | 2024-01-01 | Initial schema |

## Future Considerations

- Add `quote_volume` field for quote asset volume
- Add `trades` field for number of trades
- Add `taker_buy_volume` for buy/sell pressure analysis
- Consider adding `vwap` (volume-weighted average price)

