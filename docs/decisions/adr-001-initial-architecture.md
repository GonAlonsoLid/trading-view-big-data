# ADR-001: Initial Architecture

## Status

Accepted

## Context

We are building a trading data platform that will eventually include:
- Historical and real-time OHLCV data ingestion
- Technical indicator calculations
- REST API for data access
- WebSocket server for real-time updates
- Frontend charting application (TradingView-style)

We need an architecture that:
1. Supports incremental development
2. Allows components to be developed and deployed independently
3. Scales from local development to production deployment
4. Maintains clear boundaries between concerns

## Decision

### Project Structure

We adopt a layered architecture with clear domain boundaries:

```
/apps           - Executable applications (CLI, servers)
/packages       - Reusable libraries (clients, contracts, storage)
/pipelines      - Data pipelines (batch, streaming, spark)
/infra          - Infrastructure configuration (docker, configs)
/docs           - Documentation (contracts, ADRs)
/data_lake      - Local data storage (not versioned)
/tests          - Test suite
```

### Technology Choices (Phase 1 - Batch Ingestion)

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Language | Python 3.11 | Rich data ecosystem, team familiarity |
| HTTP Client | httpx | Modern async-capable client |
| Data Format | Parquet | Columnar, compressed, widely supported |
| Validation | Pydantic | Type safety, clear error messages |
| CLI | Click | Clean interface, easy testing |

### Future Technology Stack

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Streaming | Kafka | Industry standard, scalable |
| Batch Processing | Spark | Distributed processing, SQL interface |
| Storage | HDFS/S3 | Scalable object storage |
| API | FastAPI | Performance, OpenAPI docs |
| Frontend | React + TradingView | Rich charting, familiar tech |

### Data Partitioning Strategy

Partition by time (year/month) for:
- Query efficiency (time-range queries common)
- File size management
- Independent partition processing
- Easy data lifecycle management

### Naming Conventions

- **Symbols**: Uppercase, no separator (e.g., `BTCUSDT`)
- **Timeframes**: Lowercase with unit (e.g., `1h`, `1d`, `15m`)
- **Sources**: Lowercase (e.g., `binance`, `coinbase`)
- **Files**: `part-{NNNNN}.parquet` for data files

## Consequences

### Positive

- Clear separation allows independent development
- Parquet format enables efficient queries
- Partitioning strategy supports future scaling
- Modular design allows technology swaps

### Negative

- More directories to navigate
- Need discipline to maintain boundaries
- Some initial overhead for simple use cases

### Risks

- Parquet schema changes require migration strategy
- Partition strategy may need adjustment for very high frequency data

## Related

- [OHLCV Contract v1](../contracts/ohlcv_v1.md)

