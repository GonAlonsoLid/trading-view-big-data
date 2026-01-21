"""
Binance Klines (Candlestick) API Client.

Implements pagination, rate limiting, and retry logic for fetching
historical OHLCV data from Binance's public API.

API Documentation: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
"""

import time
from datetime import datetime, timezone
from typing import Iterator

import httpx

from packages.utils.logging import get_logger

logger = get_logger(__name__)


# Binance API constants
BINANCE_API_BASE_URL = "https://api.binance.com"
KLINES_ENDPOINT = "/api/v3/klines"
MAX_LIMIT = 1000  # Binance max records per request

# Timeframe to milliseconds mapping
TIMEFRAME_MS = {
    "1m": 60 * 1000,
    "3m": 3 * 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "2h": 2 * 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "6h": 6 * 60 * 60 * 1000,
    "8h": 8 * 60 * 60 * 1000,
    "12h": 12 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
    "3d": 3 * 24 * 60 * 60 * 1000,
    "1w": 7 * 24 * 60 * 60 * 1000,
    "1M": 30 * 24 * 60 * 60 * 1000,  # Approximate
}


class BinanceAPIError(Exception):
    """Exception raised for Binance API errors."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Binance API Error ({status_code}): {message}")


class BinanceKlinesClient:
    """
    Client for fetching historical klines (candlestick) data from Binance.
    
    Features:
    - Automatic pagination for large date ranges
    - Exponential backoff retry on rate limits (429) and server errors (5xx)
    - Yields raw kline data for memory-efficient processing
    
    Usage:
        client = BinanceKlinesClient()
        for klines_batch in client.fetch_klines(
            symbol="BTCUSDT",
            interval="1h",
            start_time=datetime(2022, 1, 1),
            end_time=datetime(2023, 1, 1)
        ):
            process(klines_batch)
    """

    def __init__(
        self,
        base_url: str = BINANCE_API_BASE_URL,
        timeout: float = 30.0,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
    ):
        """
        Initialize the Binance Klines client.
        
        Args:
            base_url: Binance API base URL
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries on failure
            base_delay: Base delay for exponential backoff (seconds)
            max_delay: Maximum delay between retries (seconds)
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._client: httpx.Client | None = None

    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                base_url=self.base_url,
                timeout=self.timeout,
                headers={"Accept": "application/json"},
            )
        return self._client

    def close(self):
        """Close the HTTP client."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _datetime_to_ms(self, dt: datetime) -> int:
        """Convert datetime to milliseconds timestamp (UTC)."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    def _request_with_retry(self, params: dict) -> list:
        """
        Make a request with exponential backoff retry.
        
        Args:
            params: Query parameters for the request
            
        Returns:
            List of kline data
            
        Raises:
            BinanceAPIError: If all retries are exhausted
        """
        client = self._get_client()
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                response = client.get(KLINES_ENDPOINT, params=params)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 429:
                    # Rate limited - extract retry-after if available
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        delay = float(retry_after)
                    else:
                        delay = min(self.base_delay * (2**attempt), self.max_delay)
                    
                    logger.warning(
                        f"Rate limited (429). Retrying in {delay:.1f}s "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(delay)
                    continue

                if response.status_code >= 500:
                    # Server error - retry with backoff
                    delay = min(self.base_delay * (2**attempt), self.max_delay)
                    logger.warning(
                        f"Server error ({response.status_code}). Retrying in {delay:.1f}s "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(delay)
                    continue

                # Client error (4xx except 429) - don't retry
                error_msg = response.text
                try:
                    error_data = response.json()
                    error_msg = error_data.get("msg", error_msg)
                except Exception:
                    pass
                raise BinanceAPIError(response.status_code, error_msg)

            except httpx.RequestError as e:
                last_exception = e
                delay = min(self.base_delay * (2**attempt), self.max_delay)
                logger.warning(
                    f"Request error: {e}. Retrying in {delay:.1f}s "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                time.sleep(delay)

        # All retries exhausted
        if last_exception:
            raise BinanceAPIError(0, f"Request failed after {self.max_retries} retries: {last_exception}")
        raise BinanceAPIError(0, f"Request failed after {self.max_retries} retries")

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = MAX_LIMIT,
    ) -> Iterator[list[list]]:
        """
        Fetch klines data with automatic pagination.
        
        Yields batches of klines to allow memory-efficient processing of large
        date ranges.
        
        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Kline interval (e.g., "1h", "1d")
            start_time: Start datetime (inclusive, UTC)
            end_time: End datetime (inclusive, UTC)
            limit: Number of records per request (max 1000)
            
        Yields:
            List of kline records. Each kline is a list:
            [
                open_time,      # 0: Kline open time (ms)
                open,           # 1: Open price
                high,           # 2: High price
                low,            # 3: Low price
                close,          # 4: Close price
                volume,         # 5: Volume
                close_time,     # 6: Kline close time (ms)
                quote_volume,   # 7: Quote asset volume
                trades,         # 8: Number of trades
                taker_buy_vol,  # 9: Taker buy base asset volume
                taker_buy_quote,# 10: Taker buy quote asset volume
                ignore          # 11: Ignore
            ]
        """
        if interval not in TIMEFRAME_MS:
            raise ValueError(f"Invalid interval: {interval}. Valid: {list(TIMEFRAME_MS.keys())}")

        limit = min(limit, MAX_LIMIT)
        interval_ms = TIMEFRAME_MS[interval]
        
        current_start_ms = self._datetime_to_ms(start_time)
        end_ms = self._datetime_to_ms(end_time)

        logger.info(
            f"Fetching klines: symbol={symbol}, interval={interval}, "
            f"start={start_time.isoformat()}, end={end_time.isoformat()}"
        )

        total_fetched = 0
        request_count = 0

        while current_start_ms < end_ms:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": current_start_ms,
                "endTime": end_ms,
                "limit": limit,
            }

            klines = self._request_with_retry(params)
            request_count += 1

            if not klines:
                # No more data
                break

            total_fetched += len(klines)
            logger.debug(
                f"Fetched {len(klines)} klines (total: {total_fetched}, requests: {request_count})"
            )

            yield klines

            # Move start time to after the last kline
            last_kline_time = klines[-1][0]  # open_time of last kline
            current_start_ms = last_kline_time + interval_ms

            # Small delay to be nice to the API
            time.sleep(0.1)

        logger.info(
            f"Completed fetching klines: total={total_fetched}, requests={request_count}"
        )

    def fetch_all_klines(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[list]:
        """
        Fetch all klines as a single list.
        
        Warning: For large date ranges, this may consume significant memory.
        Prefer using fetch_klines() iterator for large ranges.
        
        Args:
            symbol: Trading pair symbol
            interval: Kline interval
            start_time: Start datetime
            end_time: End datetime
            
        Returns:
            List of all kline records
        """
        all_klines = []
        for batch in self.fetch_klines(symbol, interval, start_time, end_time):
            all_klines.extend(batch)
        return all_klines

