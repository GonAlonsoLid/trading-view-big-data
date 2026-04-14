# -*- coding: utf-8 -*-
"""
HU-8: Consumer Kafka -> Elasticsearch para datos raw de BTC.

Lee mensajes del topic gittba_BTC y los inserta como documentos JSON
en el indice gittba_BTC de Elasticsearch usando la libreria requests.

Formato del documento insertado:
{
    "symbol": "BTCUSDT",
    "@timestamp": "2026-04-08T00:00:00Z",
    "close": 67396.99,
    "volume": 19.5
}
"""

from __future__ import annotations

import json
import sys

import requests
from requests.auth import HTTPBasicAuth
from kafka import KafkaConsumer
from kafka.structs import TopicPartition


# ── Configuracion Kafka ──────────────────────────────────────────────
BOOTSTRAP_SERVERS = "192.168.80.34:9092"
KAFKA_TOPIC = "gittba_BTC"
GROUP_ID = "gittba03_elastic_btc"

# ── Configuracion Elasticsearch ──────────────────────────────────────
ES_URL = "http://192.168.80.37:9201"
ES_INDEX = "gittba_btc"
ES_USER = "elastic"
ES_PASSWORD = "pass4icai"
ES_HEADERS = {"Content-Type": "application/json"}


def _safe_json_decode(v: bytes | None) -> dict | None:
    try:
        return json.loads(v.decode("utf-8")) if v else None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def create_index_if_not_exists() -> None:
    """Crea el indice en Elasticsearch si no existe, con mapping para @timestamp."""
    url = f"{ES_URL}/{ES_INDEX}"
    resp = requests.head(url, auth=HTTPBasicAuth(ES_USER, ES_PASSWORD))

    if resp.status_code == 200:
        print(f"Indice '{ES_INDEX}' ya existe.")
        return

    mapping = {
        "mappings": {
            "properties": {
                "symbol": {"type": "keyword"},
                "@timestamp": {"type": "date"},
                "close": {"type": "double"},
                "volume": {"type": "double"},
            }
        }
    }

    resp = requests.put(
        url,
        data=json.dumps(mapping),
        headers=ES_HEADERS,
        auth=HTTPBasicAuth(ES_USER, ES_PASSWORD),
    )

    if resp.status_code in (200, 201):
        print(f"Indice '{ES_INDEX}' creado correctamente.")
    else:
        print(f"Error creando indice: {resp.status_code} - {resp.text}")
        sys.exit(1)


def insert_document(doc: dict) -> None:
    """Inserta un documento JSON en el indice de Elasticsearch."""
    url = f"{ES_URL}/{ES_INDEX}/_doc"

    resp = requests.post(
        url,
        data=json.dumps(doc),
        headers=ES_HEADERS,
        auth=HTTPBasicAuth(ES_USER, ES_PASSWORD),
    )

    result = resp.json()
    status = "OK" if resp.status_code in (200, 201) else "ERROR"
    print(
        f"[{status}] Insertado en ES | "
        f"id={result.get('_id', 'N/A')} | "
        f"symbol={doc.get('symbol')} | "
        f"@timestamp={doc.get('@timestamp')} | "
        f"close={doc.get('close')} | "
        f"volume={doc.get('volume')}"
    )


def main() -> None:
    create_index_if_not_exists()

    consumer = KafkaConsumer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        value_deserializer=lambda v: _safe_json_decode(v),
    )

    consumer.assign([TopicPartition(KAFKA_TOPIC, 0)])

    print(f"\nEscuchando topic '{KAFKA_TOPIC}' e insertando en ES indice '{ES_INDEX}'...\n")

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            for _, consumer_records in records.items():
                for record in consumer_records:
                    value = record.value
                    if not value:
                        continue

                    doc = {
                        "symbol": value.get("symbol"),
                        "@timestamp": value.get("@timestamp"),
                        "close": value.get("close"),
                        "volume": value.get("volume"),
                    }

                    insert_document(doc)
    except KeyboardInterrupt:
        print("\nDetenido por el usuario.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
