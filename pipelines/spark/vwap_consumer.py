# -*- coding: utf-8 -*-

import json

from kafka import KafkaConsumer
from kafka.structs import TopicPartition


def _safe_json_decode(v):
    try:
        return json.loads(v.decode("utf-8")) if v else None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


BOOTSTRAP_SERVERS = "192.168.80.34:9092"
TOPIC = "gittba_BTC_VWAP"
GROUP_ID = "gittba03_vwap"


def main() -> None:
    consumer = KafkaConsumer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        value_deserializer=lambda v: _safe_json_decode(v),
    )

    consumer.assign([TopicPartition(TOPIC, 0)])

    print(f"Escuchando topic {TOPIC}...\n")

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            for topic_data, consumer_records in records.items():
                for record in consumer_records:
                    value = record.value
                    if not value:
                        continue
                    print(
                        f"offset={record.offset} | "
                        f"key={record.key} | "
                        f"window_start={value.get('window_start')} | "
                        f"window_end={value.get('window_end')} | "
                        f"symbol={value.get('symbol')} | "
                        f"vwap={value.get('vwap')}"
                    )
    except KeyboardInterrupt:
        print("\nDetenido por el usuario.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
