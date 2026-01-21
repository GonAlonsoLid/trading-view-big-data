"""
OHLCV Data Contract v1.

Defines the schema for OHLCV (Open, High, Low, Close, Volume) candlestick data.
Includes validation logic to ensure data quality.

Schema Version: 1
"""

from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field, field_validator, model_validator


class OHLCVRecord(BaseModel):
    """
    Single OHLCV candlestick record.
    
    Attributes:
        event_time: UTC timestamp of the candle open time
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        timeframe: Candle timeframe (e.g., "1h", "1d")
        open: Opening price
        high: Highest price
        low: Lowest price
        close: Closing price
        volume: Trading volume in base asset
        source: Data source identifier
        ingestion_time: UTC timestamp when the data was ingested
    """

    event_time: datetime = Field(..., description="Candle open time (UTC)")
    symbol: str = Field(..., min_length=1, description="Trading pair symbol")
    timeframe: str = Field(..., min_length=1, description="Candle timeframe")
    open: float = Field(..., ge=0, description="Opening price")
    high: float = Field(..., ge=0, description="Highest price")
    low: float = Field(..., ge=0, description="Lowest price")
    close: float = Field(..., ge=0, description="Closing price")
    volume: float = Field(..., ge=0, description="Trading volume")
    source: str = Field(default="binance", description="Data source")
    ingestion_time: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Ingestion timestamp (UTC)"
    )

    @field_validator("event_time", "ingestion_time", mode="before")
    @classmethod
    def ensure_utc(cls, v):
        """Ensure datetime is UTC."""
        if isinstance(v, datetime):
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc)
        return v

    @model_validator(mode="after")
    def validate_ohlc_consistency(self):
        """Validate OHLC price consistency."""
        # High must be >= max(open, close)
        if self.high < max(self.open, self.close):
            raise ValueError(
                f"High ({self.high}) must be >= max(open, close) ({max(self.open, self.close)})"
            )
        # Low must be <= min(open, close)
        if self.low > min(self.open, self.close):
            raise ValueError(
                f"Low ({self.low}) must be <= min(open, close) ({min(self.open, self.close)})"
            )
        # High must be >= Low
        if self.high < self.low:
            raise ValueError(f"High ({self.high}) must be >= Low ({self.low})")
        return self

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class OHLCVBatch(BaseModel):
    """
    Batch of OHLCV records with metadata.
    """

    records: list[OHLCVRecord]
    symbol: str
    timeframe: str
    source: str = "binance"
    record_count: int = 0

    @model_validator(mode="after")
    def set_record_count(self):
        """Set record count from records list."""
        object.__setattr__(self, "record_count", len(self.records))
        return self


def validate_ohlcv_batch(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Validate a DataFrame of OHLCV records.
    
    Performs the following validations:
    - No null values in required columns
    - high >= max(open, close)
    - low <= min(open, close)
    - volume >= 0
    - high >= low
    
    Args:
        df: DataFrame with OHLCV data
        
    Returns:
        Tuple of (valid_df, errors) where errors is a list of validation messages
    """
    errors = []
    required_columns = [
        "event_time", "symbol", "timeframe", 
        "open", "high", "low", "close", "volume", "source"
    ]

    # Check required columns exist
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
        return df, errors

    # Check for nulls in required columns
    null_counts = df[required_columns].isnull().sum()
    null_cols = null_counts[null_counts > 0]
    if not null_cols.empty:
        for col, count in null_cols.items():
            errors.append(f"Column '{col}' has {count} null values")

    # Validate OHLC consistency
    # high >= max(open, close)
    invalid_high = df["high"] < df[["open", "close"]].max(axis=1)
    if invalid_high.any():
        count = invalid_high.sum()
        errors.append(f"{count} records have high < max(open, close)")

    # low <= min(open, close)
    invalid_low = df["low"] > df[["open", "close"]].min(axis=1)
    if invalid_low.any():
        count = invalid_low.sum()
        errors.append(f"{count} records have low > min(open, close)")

    # high >= low
    invalid_range = df["high"] < df["low"]
    if invalid_range.any():
        count = invalid_range.sum()
        errors.append(f"{count} records have high < low")

    # volume >= 0
    invalid_volume = df["volume"] < 0
    if invalid_volume.any():
        count = invalid_volume.sum()
        errors.append(f"{count} records have negative volume")

    # Filter valid records
    valid_mask = (
        ~df[required_columns].isnull().any(axis=1) &
        (df["high"] >= df[["open", "close"]].max(axis=1)) &
        (df["low"] <= df[["open", "close"]].min(axis=1)) &
        (df["high"] >= df["low"]) &
        (df["volume"] >= 0)
    )

    return df[valid_mask].copy(), errors


def binance_kline_to_ohlcv(
    kline: list,
    symbol: str,
    timeframe: str,
    source: str = "binance",
    ingestion_time: Optional[datetime] = None,
) -> dict:
    """
    Convert a Binance kline record to OHLCV format.
    
    Binance kline format:
    [
        open_time,      # 0: Kline open time (ms)
        open,           # 1: Open price
        high,           # 2: High price
        low,            # 3: Low price
        close,          # 4: Close price
        volume,         # 5: Volume
        close_time,     # 6: Kline close time (ms)
        ...
    ]
    
    Args:
        kline: Binance kline record
        symbol: Trading pair symbol
        timeframe: Candle timeframe
        source: Data source identifier
        ingestion_time: Override ingestion timestamp
        
    Returns:
        Dictionary with OHLCV fields
    """
    if ingestion_time is None:
        ingestion_time = datetime.now(timezone.utc)

    # Convert milliseconds to UTC datetime
    event_time = datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc)

    return {
        "event_time": event_time,
        "symbol": symbol,
        "timeframe": timeframe,
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "volume": float(kline[5]),
        "source": source,
        "ingestion_time": ingestion_time,
    }


def create_ohlcv_dataframe(records: list[dict]) -> pd.DataFrame:
    """
    Create a DataFrame from OHLCV records with proper dtypes.
    
    Args:
        records: List of OHLCV dictionaries
        
    Returns:
        DataFrame with proper column types
    """
    if not records:
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=[
            "event_time", "symbol", "timeframe",
            "open", "high", "low", "close", "volume",
            "source", "ingestion_time"
        ])

    df = pd.DataFrame(records)

    # Ensure proper dtypes
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df["ingestion_time"] = pd.to_datetime(df["ingestion_time"], utc=True)
    df["symbol"] = df["symbol"].astype(str)
    df["timeframe"] = df["timeframe"].astype(str)
    df["source"] = df["source"].astype(str)

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    return df


# Schema version for documentation and migration tracking
OHLCV_SCHEMA_VERSION = "v1"

# Column order for Parquet files
OHLCV_COLUMN_ORDER = [
    "event_time",
    "symbol", 
    "timeframe",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "ingestion_time",
]

