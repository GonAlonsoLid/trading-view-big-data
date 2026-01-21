"""
Smoke tests for batch OHLCV ingestion.

These tests verify:
- Data fetching works correctly
- Schema is correct
- Deduplication and ordering work
- Parquet files are written to expected paths
"""

import os
import shutil
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import pytest

from packages.clients.binance import BinanceKlinesClient
from packages.contracts.ohlcv import (
    binance_kline_to_ohlcv,
    create_ohlcv_dataframe,
    validate_ohlcv_batch,
    OHLCV_COLUMN_ORDER,
)
from packages.storage.parquet_writer import ParquetPartitionedWriter, read_ohlcv_partition
from pipelines.batch.ohlcv_backfill import run_backfill


class TestBinanceClient:
    """Tests for Binance Klines client."""

    def test_fetch_klines_small_range(self):
        """Test fetching klines for a small date range (2 days)."""
        client = BinanceKlinesClient()

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        klines = []
        for batch in client.fetch_klines(
            symbol="BTCUSDT",
            interval="1h",
            start_time=start,
            end_time=end,
        ):
            klines.extend(batch)

        client.close()

        # 2 days * 24 hours = 48 candles (approximately)
        assert len(klines) >= 24, f"Expected at least 24 klines, got {len(klines)}"
        assert len(klines) <= 50, f"Expected at most 50 klines, got {len(klines)}"

        # Check kline structure
        kline = klines[0]
        assert len(kline) >= 11, "Kline should have at least 11 elements"
        assert isinstance(kline[0], int), "Open time should be int (timestamp)"
        assert isinstance(kline[1], str), "Open price should be string"

    def test_invalid_interval(self):
        """Test that invalid interval raises error."""
        client = BinanceKlinesClient()

        with pytest.raises(ValueError, match="Invalid interval"):
            list(client.fetch_klines(
                symbol="BTCUSDT",
                interval="invalid",
                start_time=datetime.now(timezone.utc),
                end_time=datetime.now(timezone.utc),
            ))

        client.close()


class TestOHLCVContract:
    """Tests for OHLCV data contract."""

    def test_binance_kline_conversion(self):
        """Test conversion from Binance kline format."""
        # Sample Binance kline
        kline = [
            1704067200000,  # Open time (2024-01-01 00:00:00 UTC)
            "42000.00",     # Open
            "42500.00",     # High
            "41800.00",     # Low
            "42200.00",     # Close
            "1000.5",       # Volume
            1704070799999,  # Close time
            "42100000.00",  # Quote volume
            5000,           # Trades
            "500.25",       # Taker buy base
            "21050000.00",  # Taker buy quote
            "0",            # Ignore
        ]

        record = binance_kline_to_ohlcv(
            kline=kline,
            symbol="BTCUSDT",
            timeframe="1h",
        )

        assert record["symbol"] == "BTCUSDT"
        assert record["timeframe"] == "1h"
        assert record["open"] == 42000.00
        assert record["high"] == 42500.00
        assert record["low"] == 41800.00
        assert record["close"] == 42200.00
        assert record["volume"] == 1000.5
        assert record["source"] == "binance"
        assert record["event_time"].year == 2024
        assert record["event_time"].month == 1
        assert record["event_time"].day == 1

    def test_create_dataframe(self):
        """Test DataFrame creation with proper dtypes."""
        records = [
            {
                "event_time": datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                "symbol": "BTCUSDT",
                "timeframe": "1h",
                "open": 42000.0,
                "high": 42500.0,
                "low": 41800.0,
                "close": 42200.0,
                "volume": 1000.0,
                "source": "binance",
                "ingestion_time": datetime.now(timezone.utc),
            }
        ]

        df = create_ohlcv_dataframe(records)

        assert len(df) == 1
        assert list(df.columns) == OHLCV_COLUMN_ORDER
        assert df["event_time"].dtype == "datetime64[ns, UTC]"
        assert df["open"].dtype == "float64"

    def test_validation_valid_data(self):
        """Test validation passes for valid data."""
        df = pd.DataFrame([
            {
                "event_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "symbol": "BTCUSDT",
                "timeframe": "1h",
                "open": 42000.0,
                "high": 42500.0,
                "low": 41800.0,
                "close": 42200.0,
                "volume": 1000.0,
                "source": "binance",
            }
        ])

        valid_df, errors = validate_ohlcv_batch(df)

        assert len(valid_df) == 1
        assert len(errors) == 0

    def test_validation_invalid_high(self):
        """Test validation catches high < max(open, close)."""
        df = pd.DataFrame([
            {
                "event_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "symbol": "BTCUSDT",
                "timeframe": "1h",
                "open": 42000.0,
                "high": 41000.0,  # Invalid: lower than open
                "low": 40000.0,
                "close": 42200.0,
                "volume": 1000.0,
                "source": "binance",
            }
        ])

        valid_df, errors = validate_ohlcv_batch(df)

        assert len(valid_df) == 0
        assert any("high < max(open, close)" in e for e in errors)


class TestParquetWriter:
    """Tests for Parquet partitioned writer."""

    def test_write_and_read(self):
        """Test writing and reading partitioned data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ParquetPartitionedWriter(
                base_path=tmpdir,
                source="binance",
                symbol="BTCUSDT",
                timeframe="1h",
                max_rows_per_file=100,
            )

            # Create test data
            records = []
            for i in range(50):
                records.append({
                    "event_time": datetime(2024, 1, 1, i % 24, tzinfo=timezone.utc),
                    "symbol": "BTCUSDT",
                    "timeframe": "1h",
                    "open": 42000.0 + i,
                    "high": 42500.0 + i,
                    "low": 41800.0 + i,
                    "close": 42200.0 + i,
                    "volume": 1000.0 + i,
                    "source": "binance",
                    "ingestion_time": datetime.now(timezone.utc),
                })

            df = create_ohlcv_dataframe(records)
            written_files = writer.write(df)

            assert len(written_files) > 0

            # Verify partition structure
            expected_path = Path(tmpdir) / "raw" / "ohlcv" / "source=binance" / "symbol=BTCUSDT" / "timeframe=1h" / "year=2024" / "month=01"
            assert expected_path.exists()

            # Read back
            read_df = read_ohlcv_partition(
                base_path=tmpdir,
                source="binance",
                symbol="BTCUSDT",
                timeframe="1h",
            )

            # Deduplication removes duplicates (same hour repeated)
            assert len(read_df) == 24  # Only 24 unique hours

    def test_deduplication(self):
        """Test that duplicate records are removed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            writer = ParquetPartitionedWriter(
                base_path=tmpdir,
                source="binance",
                symbol="BTCUSDT",
                timeframe="1h",
            )

            # Create duplicate records
            records = []
            for _ in range(3):  # 3 duplicates of same time
                records.append({
                    "event_time": datetime(2024, 1, 1, 0, tzinfo=timezone.utc),
                    "symbol": "BTCUSDT",
                    "timeframe": "1h",
                    "open": 42000.0,
                    "high": 42500.0,
                    "low": 41800.0,
                    "close": 42200.0,
                    "volume": 1000.0,
                    "source": "binance",
                    "ingestion_time": datetime.now(timezone.utc),
                })

            df = create_ohlcv_dataframe(records)
            writer.write(df)

            read_df = read_ohlcv_partition(
                base_path=tmpdir,
                source="binance",
                symbol="BTCUSDT",
                timeframe="1h",
            )

            assert len(read_df) == 1  # Only one record after dedup


class TestBackfillPipeline:
    """Integration tests for the backfill pipeline."""

    def test_small_backfill(self):
        """
        Smoke test: Run backfill for 2 days and verify results.
        
        This test:
        1. Fetches real data from Binance for 2 days
        2. Validates schema and data quality
        3. Verifies Parquet files are created correctly
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Run backfill for 2 days
            start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end_date = datetime(2024, 1, 2, tzinfo=timezone.utc)

            stats = run_backfill(
                symbol="BTCUSDT",
                timeframe="1h",
                start_date=start_date,
                end_date=end_date,
                output_path=tmpdir,
                max_rows_per_file=100,
                show_progress=False,
            )

            # Verify stats
            assert stats["total_records"] > 0, "Should have fetched records"
            assert stats["valid_records"] > 0, "Should have valid records"
            assert len(stats["files_written"]) > 0, "Should have written files"

            # Verify files exist
            for f in stats["files_written"]:
                assert os.path.exists(f), f"File should exist: {f}"

            # Read and verify data
            df = read_ohlcv_partition(
                base_path=tmpdir,
                source="binance",
                symbol="BTCUSDT",
                timeframe="1h",
            )

            # Verify schema
            assert list(df.columns) == OHLCV_COLUMN_ORDER

            # Verify ordering (ascending by event_time)
            assert df["event_time"].is_monotonic_increasing

            # Verify no duplicates
            duplicates = df.duplicated(subset=["event_time", "timeframe"])
            assert not duplicates.any(), "Should have no duplicates"

            # Verify data quality
            assert (df["high"] >= df[["open", "close"]].max(axis=1)).all()
            assert (df["low"] <= df[["open", "close"]].min(axis=1)).all()
            assert (df["volume"] >= 0).all()

            print(f"\nSmoke test passed!")
            print(f"  Records: {len(df)}")
            print(f"  Date range: {df['event_time'].min()} to {df['event_time'].max()}")
            print(f"  Files: {len(stats['files_written'])}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

