"""
OHLCV Backfill Pipeline.

Orchestrates the batch ingestion of historical OHLCV data from exchanges
to local Parquet storage.

Features:
- Fetches data from Binance API with automatic pagination
- Converts to OHLCV contract format with validation
- Writes to partitioned Parquet files
- Progress tracking and logging
"""

from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from tqdm import tqdm

from packages.clients.binance import BinanceKlinesClient
from packages.contracts.ohlcv import (
    binance_kline_to_ohlcv,
    create_ohlcv_dataframe,
    validate_ohlcv_batch,
    OHLCV_COLUMN_ORDER,
)
from packages.storage.parquet_writer import ParquetPartitionedWriter
from packages.utils.logging import get_logger

logger = get_logger(__name__)


# Timeframe to estimated candles per day (for progress estimation)
CANDLES_PER_DAY = {
    "1m": 1440,
    "3m": 480,
    "5m": 288,
    "15m": 96,
    "30m": 48,
    "1h": 24,
    "2h": 12,
    "4h": 6,
    "6h": 4,
    "8h": 3,
    "12h": 2,
    "1d": 1,
}


class OHLCVBackfillPipeline:
    """
    Pipeline for backfilling historical OHLCV data.
    
    Orchestrates:
    1. Data fetching from exchange API
    2. Transformation to OHLCV contract format
    3. Validation
    4. Persistence to partitioned Parquet
    
    Usage:
        pipeline = OHLCVBackfillPipeline(
            symbol="BTCUSDT",
            timeframe="1h",
            output_path="data_lake",
            max_rows_per_file=200000
        )
        stats = pipeline.run(
            start_date=datetime(2022, 1, 1),
            end_date=datetime(2025, 12, 31)
        )
    """

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        timeframe: str = "1h",
        output_path: str = "data_lake",
        source: str = "binance",
        max_rows_per_file: int = 200000,
        overwrite: bool = False,
    ):
        """
        Initialize the backfill pipeline.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe (1h, 1d, etc.)
            output_path: Base path for output data
            source: Data source identifier
            max_rows_per_file: Maximum rows per Parquet file
            overwrite: If True, overwrite existing files
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.output_path = output_path
        self.source = source
        self.max_rows_per_file = max_rows_per_file
        self.overwrite = overwrite

        # Initialize components
        self._client: Optional[BinanceKlinesClient] = None
        self._writer: Optional[ParquetPartitionedWriter] = None

    def _get_client(self) -> BinanceKlinesClient:
        """Get or create the Binance client."""
        if self._client is None:
            self._client = BinanceKlinesClient()
        return self._client

    def _get_writer(self) -> ParquetPartitionedWriter:
        """Get or create the Parquet writer."""
        if self._writer is None:
            self._writer = ParquetPartitionedWriter(
                base_path=self.output_path,
                source=self.source,
                symbol=self.symbol,
                timeframe=self.timeframe,
                max_rows_per_file=self.max_rows_per_file,
                overwrite=self.overwrite,
            )
        return self._writer

    def _estimate_total_candles(
        self, start_date: datetime, end_date: datetime
    ) -> int:
        """Estimate total number of candles for progress bar."""
        days = (end_date - start_date).days + 1
        candles_per_day = CANDLES_PER_DAY.get(self.timeframe, 24)
        return days * candles_per_day

    def run(
        self,
        start_date: datetime,
        end_date: datetime,
        show_progress: bool = True,
    ) -> dict:
        """
        Execute the backfill pipeline.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            show_progress: Show progress bar
            
        Returns:
            Dictionary with execution statistics
        """
        logger.info(
            f"Starting OHLCV backfill: symbol={self.symbol}, timeframe={self.timeframe}, "
            f"start={start_date.date()}, end={end_date.date()}"
        )

        # Ensure dates are UTC
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        # Set end_date to end of day
        end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)

        client = self._get_client()
        writer = self._get_writer()
        ingestion_time = datetime.now(timezone.utc)

        # Statistics
        stats = {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_records": 0,
            "valid_records": 0,
            "invalid_records": 0,
            "files_written": [],
            "validation_errors": [],
            "duration_seconds": 0,
        }

        start_time = datetime.now(timezone.utc)

        # Estimate total for progress bar
        estimated_total = self._estimate_total_candles(start_date, end_date)

        # Collect all records
        all_records = []
        
        try:
            # Setup progress bar
            pbar = None
            if show_progress:
                pbar = tqdm(
                    total=estimated_total,
                    desc=f"Fetching {self.symbol} {self.timeframe}",
                    unit="candles",
                )

            # Fetch data from API
            for batch in client.fetch_klines(
                symbol=self.symbol,
                interval=self.timeframe,
                start_time=start_date,
                end_time=end_date,
            ):
                # Convert batch to OHLCV records
                for kline in batch:
                    record = binance_kline_to_ohlcv(
                        kline=kline,
                        symbol=self.symbol,
                        timeframe=self.timeframe,
                        source=self.source,
                        ingestion_time=ingestion_time,
                    )
                    all_records.append(record)

                if pbar:
                    pbar.update(len(batch))

            if pbar:
                pbar.close()

            stats["total_records"] = len(all_records)
            logger.info(f"Fetched {len(all_records)} total records")

            if not all_records:
                logger.warning("No records fetched, nothing to write")
                return stats

            # Create DataFrame
            df = create_ohlcv_dataframe(all_records)

            # Validate data
            logger.info("Validating data...")
            valid_df, validation_errors = validate_ohlcv_batch(df)

            stats["valid_records"] = len(valid_df)
            stats["invalid_records"] = len(df) - len(valid_df)
            stats["validation_errors"] = validation_errors

            if validation_errors:
                for error in validation_errors:
                    logger.warning(f"Validation: {error}")

            if valid_df.empty:
                logger.error("No valid records after validation")
                return stats

            # Ensure column order
            valid_df = valid_df[OHLCV_COLUMN_ORDER]

            # Write to Parquet
            logger.info(f"Writing {len(valid_df)} valid records to Parquet...")
            written_files = writer.write(valid_df)
            stats["files_written"] = written_files

            end_time = datetime.now(timezone.utc)
            stats["duration_seconds"] = (end_time - start_time).total_seconds()

            logger.info(
                f"Backfill complete: {stats['valid_records']} records written to "
                f"{len(written_files)} files in {stats['duration_seconds']:.1f}s"
            )

            return stats

        finally:
            if self._client:
                self._client.close()


def run_backfill(
    symbol: str = "BTCUSDT",
    timeframe: str = "1h",
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    output_path: str = "data_lake",
    max_rows_per_file: int = 200000,
    overwrite: bool = False,
    show_progress: bool = True,
) -> dict:
    """
    Convenience function to run the backfill pipeline.
    
    Args:
        symbol: Trading pair symbol
        timeframe: Candle timeframe
        start_date: Start date (default: 2022-01-01)
        end_date: End date (default: 2025-12-31)
        output_path: Output path for data
        max_rows_per_file: Max rows per file
        overwrite: Overwrite existing files
        show_progress: Show progress bar
        
    Returns:
        Execution statistics
    """
    if start_date is None:
        start_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
    if end_date is None:
        end_date = datetime(2025, 12, 31, tzinfo=timezone.utc)

    pipeline = OHLCVBackfillPipeline(
        symbol=symbol,
        timeframe=timeframe,
        output_path=output_path,
        max_rows_per_file=max_rows_per_file,
        overwrite=overwrite,
    )

    return pipeline.run(
        start_date=start_date,
        end_date=end_date,
        show_progress=show_progress,
    )

