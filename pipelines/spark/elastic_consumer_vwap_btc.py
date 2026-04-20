# -*- coding: utf-8 -*-
"""
HU-8: Consumer Kafka -> Elasticsearch para datos VWAP de BTC.

Lee mensajes del topic gittba_BTC_VWAP y los inserta como documentos JSON
en el indice gittba_BTC_VWAP de Elasticsearch usando la libreria requests.

El campo @timestamp se deriva de window_end para que Kibana pueda
usar el campo de tiempo nativo.

Formato del documento insertado:
{
    "window_start": "2026-03-16T14:20:00.000Z",
    "window_end": "2026-03-16T14:25:00.000Z",
    "symbol": "BTCUSDT",
    "vwap": 74229.99,
    "@timestamp": "2026-03-16T14:25:00Z"
}
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import requests
from requests.auth import HTTPBasicAuth
from kafka import KafkaConsumer
from kafka.structs import TopicPartition


# ── Configuracion Kafka ──────────────────────────────────────────────
BOOTSTRAP_SERVERS = "192.168.80.34:9092"
KAFKA_TOPIC = "gittba_BTC_VWAP"
GROUP_ID = "gittba03_elastic_vwap"

# ── Configuracion Elasticsearch ──────────────────────────────────────
ES_URL = "http://192.168.80.37:9201"
ES_INDEX = "gittba_btc_vwap"
ES_USER = "elastic"
ES_PASSWORD = "pass4icai"
ES_HEADERS = {"Content-Type": "application/json"}


def _safe_json_decode(v: bytes | None) -> dict | None:
    try:
        return json.loads(v.decode("utf-8")) if v else None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _to_es_timestamp(window_end: str) -> str:
    """Convierte window_end de Spark a formato ISO 8601 para @timestamp en ES.

    Spark emite timestamps como '2026-03-16 14:25:00' o con 'T'.
    Normalizamos al formato requerido: '2026-03-16T14:25:00Z'.
    """
    cleaned = window_end.replace("T", " ").replace("Z", "").strip()
    # Manejar milisegundos opcionales (.000)
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(cleaned, fmt).replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    return window_end


def create_index_if_not_exists() -> None:
    """Crea el indice en Elasticsearch si no existe, con mapping para VWAP."""
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
                "window_start": {"type": "date"},
                "window_end": {"type": "date"},
                "vwap": {"type": "double"},
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
    if status == "ERROR":
        print(f"[ERROR] {resp.status_code} - {resp.text}")
    print(
        f"[{status}] Insertado en ES | "
        f"id={result.get('_id', 'N/A')} | "
        f"symbol={doc.get('symbol')} | "
        f"@timestamp={doc.get('@timestamp')} | "
        f"vwap={doc.get('vwap')}"
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

                    window_start = value.get("window_start", "")
                    window_end = value.get("window_end", "")

                    doc = {
                        "window_start": _to_es_timestamp(window_start),
                        "window_end": _to_es_timestamp(window_end),
                        "symbol": value.get("symbol"),
                        "vwap": value.get("vwap"),
                        "@timestamp": _to_es_timestamp(window_end),
                    }

                    insert_document(doc)
    except KeyboardInterrupt:
        print("\nDetenido por el usuario.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
