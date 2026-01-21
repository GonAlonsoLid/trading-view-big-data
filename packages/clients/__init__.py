"""
External API clients.

Available clients:
- binance: Binance exchange client for market data
"""

from packages.clients.binance import BinanceKlinesClient

__all__ = ["BinanceKlinesClient"]

