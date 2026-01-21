"""
Parquet writer with partitioning support.

Writes DataFrames to Parquet format with automatic partitioning by time
(year/month) and file splitting for large partitions.

Output structure:
    data_lake/raw/ohlcv/source=<source>/symbol=<symbol>/timeframe=<tf>/
        year=YYYY/month=MM/part-00000.parquet
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from packages.utils.logging import get_logger

logger = get_logger(__name__)


class ParquetPartitionedWriter:
    """
    Parquet writer with time-based partitioning and automatic file splitting.
    
    Features:
    - Partitions data by year/month based on event_time
    - Splits files when row count exceeds threshold
    - Deduplicates records by (event_time, timeframe)
    - Orders records by event_time ascending
    
    Usage:
        writer = ParquetPartitionedWriter(
            base_path="data_lake",
            source="binance",
            symbol="BTCUSDT",
            timeframe="1h",
            max_rows_per_file=200000
        )
        writer.write(df)
    """

    def __init__(
        self,
        base_path: str,
        source: str,
        symbol: str,
        timeframe: str,
        max_rows_per_file: int = 200000,
        overwrite: bool = False,
    ):
        """
        Initialize the Parquet writer.
        
        Args:
            base_path: Base path for the data lake (e.g., "data_lake")
            source: Data source identifier (e.g., "binance")
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            timeframe: Candle timeframe (e.g., "1h", "1d")
            max_rows_per_file: Maximum rows per Parquet file before splitting
            overwrite: If True, overwrite existing files
        """
        self.base_path = Path(base_path)
        self.source = source
        self.symbol = symbol
        self.timeframe = timeframe
        self.max_rows_per_file = max_rows_per_file
        self.overwrite = overwrite

        # Track written files
        self._written_files: list[str] = []

    def _get_partition_path(self, year: int, month: int) -> Path:
        """
        Get the path for a specific year/month partition.
        
        Args:
            year: Year
            month: Month (1-12)
            
        Returns:
            Path to the partition directory
        """
        return (
            self.base_path
            / "raw"
            / "ohlcv"
            / f"source={self.source}"
            / f"symbol={self.symbol}"
            / f"timeframe={self.timeframe}"
            / f"year={year}"
            / f"month={month:02d}"
        )

    def _get_next_part_number(self, partition_path: Path) -> int:
        """
        Get the next available part number for a partition.
        
        Args:
            partition_path: Path to the partition directory
            
        Returns:
            Next part number (0 if no existing files)
        """
        if not partition_path.exists():
            return 0

        if self.overwrite:
            return 0

        existing_files = list(partition_path.glob("part-*.parquet"))
        if not existing_files:
            return 0

        # Extract part numbers and find max
        part_numbers = []
        for f in existing_files:
            try:
                part_num = int(f.stem.split("-")[1])
                part_numbers.append(part_num)
            except (IndexError, ValueError):
                continue

        return max(part_numbers) + 1 if part_numbers else 0

    def _write_partition(
        self,
        df: pd.DataFrame,
        year: int,
        month: int,
    ) -> list[str]:
        """
        Write data for a single partition, splitting if necessary.
        
        Args:
            df: DataFrame to write
            year: Year
            month: Month
            
        Returns:
            List of written file paths
        """
        partition_path = self._get_partition_path(year, month)
        partition_path.mkdir(parents=True, exist_ok=True)

        written_files = []
        start_part = self._get_next_part_number(partition_path) if not self.overwrite else 0

        # Split into chunks if necessary
        n_chunks = (len(df) + self.max_rows_per_file - 1) // self.max_rows_per_file

        for i in range(n_chunks):
            start_idx = i * self.max_rows_per_file
            end_idx = min((i + 1) * self.max_rows_per_file, len(df))
            chunk_df = df.iloc[start_idx:end_idx]

            part_num = start_part + i
            file_path = partition_path / f"part-{part_num:05d}.parquet"

            # Convert to PyArrow Table and write
            table = pa.Table.from_pandas(chunk_df, preserve_index=False)
            pq.write_table(
                table,
                file_path,
                compression="snappy",
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )

            written_files.append(str(file_path))
            logger.debug(f"Written {len(chunk_df)} rows to {file_path}")

        return written_files

    def write(self, df: pd.DataFrame) -> list[str]:
        """
        Write DataFrame to partitioned Parquet files.
        
        The data is:
        1. Deduplicated by (event_time, timeframe)
        2. Sorted by event_time ascending
        3. Partitioned by year/month
        4. Split into multiple files if exceeding max_rows_per_file
        
        Args:
            df: DataFrame with OHLCV data
            
        Returns:
            List of written file paths
        """
        if df.empty:
            logger.warning("Empty DataFrame, nothing to write")
            return []

        # Ensure event_time is datetime with UTC
        if not pd.api.types.is_datetime64_any_dtype(df["event_time"]):
            df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
        elif df["event_time"].dt.tz is None:
            df["event_time"] = df["event_time"].dt.tz_localize("UTC")

        # Deduplicate by (event_time, timeframe)
        initial_count = len(df)
        df = df.drop_duplicates(subset=["event_time", "timeframe"], keep="last")
        if len(df) < initial_count:
            logger.info(f"Removed {initial_count - len(df)} duplicate records")

        # Sort by event_time ascending
        df = df.sort_values("event_time").reset_index(drop=True)

        # Extract year and month for partitioning
        df["_year"] = df["event_time"].dt.year
        df["_month"] = df["event_time"].dt.month

        written_files = []

        # Group by year/month and write each partition
        for (year, month), group_df in df.groupby(["_year", "_month"]):
            # Remove partition columns before writing
            partition_df = group_df.drop(columns=["_year", "_month"])

            files = self._write_partition(partition_df, int(year), int(month))
            written_files.extend(files)

            logger.info(
                f"Partition {year}-{month:02d}: {len(partition_df)} rows -> "
                f"{len(files)} file(s)"
            )

        self._written_files.extend(written_files)
        logger.info(f"Total files written: {len(written_files)}")

        return written_files

    @property
    def written_files(self) -> list[str]:
        """Get list of all files written by this writer."""
        return self._written_files.copy()


def read_ohlcv_partition(
    base_path: str,
    source: str,
    symbol: str,
    timeframe: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> pd.DataFrame:
    """
    Read OHLCV data from partitioned Parquet files.
    
    Args:
        base_path: Base path for the data lake
        source: Data source
        symbol: Trading pair symbol
        timeframe: Candle timeframe
        year: Optional year filter
        month: Optional month filter (requires year)
        
    Returns:
        DataFrame with OHLCV data
    """
    base = Path(base_path)
    partition_path = (
        base
        / "raw"
        / "ohlcv"
        / f"source={source}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )

    if not partition_path.exists():
        logger.warning(f"Partition path does not exist: {partition_path}")
        return pd.DataFrame()

    # Build glob pattern
    if year is not None and month is not None:
        pattern = f"year={year}/month={month:02d}/part-*.parquet"
    elif year is not None:
        pattern = f"year={year}/month=*/part-*.parquet"
    else:
        pattern = "year=*/month=*/part-*.parquet"

    parquet_files = list(partition_path.glob(pattern))

    if not parquet_files:
        logger.warning(f"No Parquet files found matching pattern: {pattern}")
        return pd.DataFrame()

    # Read all files
    dfs = []
    for f in sorted(parquet_files):
        df = pd.read_parquet(f)
        dfs.append(df)

    result = pd.concat(dfs, ignore_index=True)

    # Ensure proper sorting
    result = result.sort_values("event_time").reset_index(drop=True)

    logger.info(f"Read {len(result)} rows from {len(parquet_files)} files")

    return result

