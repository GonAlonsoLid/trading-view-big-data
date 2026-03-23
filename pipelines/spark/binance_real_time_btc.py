# -*- coding: utf-8 -*-

import json
from datetime import datetime, timezone

from binance import Client
from binance import ThreadedWebsocketManager
from kafka import KafkaProducer


BOOTSTRAP_SERVERS = "192.168.80.34:9092"
TOPIC = "gittba_BTC"
SYMBOL = "BTCUSDT"


def to_utc_string(timestamp_ms: int) -> str:
    """Convierte timestamp en milisegundos a formato UTC ISO 8601."""
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda v: v.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    def handle_kline(msg):
        kline = msg.get("k", {})

        # Solo publicar cuando la vela este cerrada
        if not kline.get("x", False):
            return

        close_time_ms = int(kline["T"])
        payload = {
            "symbol": kline["s"],
            "@timestamp": to_utc_string(close_time_ms),
            "close": float(kline["c"]),
            "volume": float(kline["v"]),
        }

        producer.send(
            TOPIC,
            key=kline["s"],
            value=payload,
            timestamp_ms=close_time_ms,
        )
        producer.flush()

        print(
            f"Publicado en {TOPIC} | "
            f"key={kline['s']} | "
            f"ts_ms={close_time_ms} | "
            f"value={payload}"
        )

    twm = ThreadedWebsocketManager()
    twm.start()

    twm.start_kline_socket(
        symbol=SYMBOL,
        interval=Client.KLINE_INTERVAL_1MINUTE,
        callback=handle_kline,
    )

    try:
        input("Escuchando Binance y publicando en Kafka. Pulsa ENTER para salir.\n")
    finally:
        twm.stop()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
