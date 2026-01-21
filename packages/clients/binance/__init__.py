"""
Binance API client for market data.

Implements:
- Klines (candlestick) data retrieval
- Rate limiting and retry logic
- Pagination for large date ranges
"""

from packages.clients.binance.klines_client import BinanceKlinesClient

__all__ = ["BinanceKlinesClient"]

