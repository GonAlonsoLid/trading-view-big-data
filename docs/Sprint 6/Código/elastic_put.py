import requests
import json
import time
import random
from datetime import datetime, timezone
from requests.auth import HTTPBasicAuth

url = 'http://192.168.80.37:9201/gittba00_test/_doc'
headers = { 'Content-Type': 'application/json' }
user = 'user'
password = 'password'

# Inserta documentos en el indice gittba00_test de Elasticsearch con el siguiente formato. No indicamos el id, lo genera ES
# {
#    'symbol': 'BTCUSDT',
#    '@timestamp': '2026-05-08T11:21:00Z',
#    'close': 67396.99,
#    'volume': 19.5
# }

class ElasticWriter:

    def write_documents(self):

        timestamp = self._current_time()
        dt_object = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        iso_format = dt_object.strftime('%Y-%m-%dT%H:%M:%SZ')
        close = round(random.uniform(0, 90000), 2)
        volume = round(random.uniform(0, 10000), 0)

        doc = {
            'symbol': 'BTCUSDT',
            '@timestamp': iso_format,
            'close': close,
            'volume': volume
        }

        print("POST document")
        response = requests.post(
            url,
            data=json.dumps(doc),
            headers=headers,
            auth=HTTPBasicAuth(user, password))

        res = response.json()
        print(res)

    @staticmethod
    def _current_time():
        return int(round(time.time()))

def main():
    writer = ElasticWriter()

    # Escribe documentos cada 60 segundos
    try:
        while True:
            writer.write_documents()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nStopping the writer...")


if __name__ == "__main__":
    main()
