# Sprint 6

**Grupo 3**

Tecnologias de Datos Masivos / Big Data Technology

---

## Table of Contents

- Introduccion
  - Contexto
  - Descripcion del problema y objetivos del sprint
  - Justificacion
- Metodologia
  - Entorno de Desarrollo
  - Pruebas realizadas
  - Resultados
- Conclusion
- Anexo

---

## Introduccion

### Contexto

En los sprints anteriores se construyo un pipeline de streaming completo: desde la captura de datos de cotizacion de BTC en tiempo real via Binance WebSocket, pasando por la publicacion en Apache Kafka, hasta el calculo del indicador VWAP mediante Spark Structured Streaming. Sin embargo, los datos procesados quedaban almacenados unicamente en topics de Kafka, sin posibilidad de consulta interactiva ni visualizacion.

Para cerrar el ciclo de la arquitectura de procesamiento streaming propuesta en la asignatura (Data Acquisition -> Data Storage -> Data Analysis -> Results), falta la capa de resultados: un almacen de datos indexado que permita consultar y visualizar la informacion en tiempo real.

### Descripcion del problema y objetivos del sprint

El objetivo de este sprint es implementar la capa de visualizacion y almacenamiento final del pipeline de streaming, correspondiente a la Historia de Usuario 8 (HU-8). En concreto:

- **Creacion de indices en Elasticsearch:** se crean dos indices por criptomoneda, uno para los datos raw de cotizacion (`gittba_BTC`) y otro para los resultados del calculo VWAP (`gittba_BTC_VWAP`). Ambos indices incluyen un mapping explicito con el campo `@timestamp` de tipo `date`, necesario para que Kibana pueda utilizar el filtro temporal nativo.

- **Consumers Kafka -> Elasticsearch:** se desarrollan dos scripts Python que consumen mensajes de los topics de Kafka correspondientes y los insertan como documentos JSON en Elasticsearch utilizando la libreria `requests` con autenticacion basica HTTP, tal y como indica la historia de usuario.

- **Dashboard en Kibana:** se configuran Data Views y un dashboard interactivo en Kibana para visualizar en tiempo real los datos de cotizacion y VWAP almacenados en Elasticsearch.

### Justificacion

Elasticsearch es el motor de busqueda y analisis distribuido del Elastic Stack, optimizado para consultas en tiempo real sobre grandes volumenes de datos. Su API RESTful permite la integracion directa desde cualquier lenguaje, y su compatibilidad nativa con Kibana facilita la creacion de dashboards sin necesidad de herramientas adicionales.

La decision de usar la libreria `requests` (en lugar del cliente oficial `elasticsearch-py`) responde al requisito explicito de la HU-8. Este enfoque ofrece transparencia total sobre las llamadas HTTP realizadas, lo que facilita la comprension del protocolo de comunicacion con Elasticsearch y el diagnostico de errores.

El campo `@timestamp` se incluye en todos los documentos para que Kibana pueda aplicar filtros temporales y ordenar cronologicamente los datos tanto en Discover como en las visualizaciones del dashboard.

---

## Metodologia

### Entorno de Desarrollo

El pipeline se ha implementado y ejecutado sobre el cluster del entorno de practicas del ICAI. Los consumers se han desarrollado en Python, utilizando las librerias `kafka-python` y `requests`, que ya se encontraban disponibles en el entorno.

Las tecnologias y componentes utilizados son:

- **Apache Kafka** (broker: 192.168.80.34:9092) como fuente de datos en streaming. Los topics consumidos son `gittba_BTC` (datos raw) y `gittba_BTC_VWAP` (VWAP calculado).
- **Elasticsearch** (nodo: 192.168.80.37:9201) como almacen de datos indexado. Se utilizan dos indices con mapping explicito para garantizar el tipado correcto de los campos.
- **Kibana** (http://192.168.80.37:5601) como herramienta de visualizacion. Se crean Data Views sobre los indices y un dashboard con graficas de lineas temporales.
- **Python 3 + requests** para los consumers que insertan documentos en Elasticsearch mediante llamadas POST a la API REST.
- **HTTPBasicAuth** para la autenticacion contra el cluster de Elasticsearch.

### Estructura de los scripts

Se han desarrollado dos scripts independientes, siguiendo la misma estructura que los consumers del sprint anterior:

| Script | Topic Kafka | Indice ES | Descripcion |
|--------|-------------|-----------|-------------|
| `elastic_consumer_btc.py` | `gittba_BTC` | `gittba_BTC` | Consume datos raw (symbol, @timestamp, close, volume) e inserta en ES |
| `elastic_consumer_vwap_btc.py` | `gittba_BTC_VWAP` | `gittba_BTC_VWAP` | Consume datos VWAP (window_start, window_end, symbol, vwap) e inserta en ES con @timestamp derivado de window_end |

Ambos scripts:
1. Comprueban si el indice existe en Elasticsearch y lo crean con mapping si no existe.
2. Se suscriben al topic de Kafka con un consumer group dedicado.
3. Por cada mensaje recibido, construyen el documento JSON y lo insertan via POST.
4. Imprimen en consola el resultado de cada insercion para facilitar la depuracion.

### Mapping de los indices

**Indice `gittba_BTC`:**
```json
{
  "mappings": {
    "properties": {
      "symbol":     { "type": "keyword" },
      "@timestamp": { "type": "date" },
      "close":      { "type": "double" },
      "volume":     { "type": "double" }
    }
  }
}
```

**Indice `gittba_BTC_VWAP`:**
```json
{
  "mappings": {
    "properties": {
      "symbol":       { "type": "keyword" },
      "@timestamp":   { "type": "date" },
      "window_start": { "type": "date" },
      "window_end":   { "type": "date" },
      "vwap":         { "type": "double" }
    }
  }
}
```

### Pruebas realizadas

Las pruebas se realizaron ejecutando los componentes del pipeline de forma secuencial sobre el cluster:

1. **Verificacion de conectividad con Elasticsearch:** se comprobo el estado del cluster mediante `curl -X GET http://192.168.80.37:9201/_cat/health?v`.

2. **Creacion de indices:** al lanzar cada consumer por primera vez, el script verifica si el indice existe y lo crea automaticamente con el mapping definido. Se confirmo la creacion correcta listando los indices con `curl -X GET http://192.168.80.37:9201/_cat/indices?v`.

3. **Insercion de datos raw:** con el productor de BTC activo (`binance_real_time_btc.py`), se lanzo `elastic_consumer_btc.py`. Se verifico que los documentos se insertaban correctamente en el indice `gittba_BTC`, con el campo `@timestamp` en formato ISO 8601.

4. **Insercion de datos VWAP:** con el job de Spark Streaming activo (`spark_streaming_vwap_btc.py`), se lanzo `elastic_consumer_vwap_btc.py`. Se verifico que los documentos VWAP se insertaban en el indice `gittba_BTC_VWAP`, con `@timestamp` derivado del campo `window_end`.

5. **Configuracion de Kibana:**
   - Se accedio a Kibana en http://192.168.80.37:5601.
   - Se crearon dos Data Views: uno para `gittba_BTC*` y otro para `gittba_BTC_VWAP*`, ambos con `@timestamp` como campo temporal.
   - Se verifico en Discover que los documentos aparecian con la cronologia correcta.
   - Se creo un dashboard con visualizaciones de lineas temporales para close, volume y vwap.

### Resultados

Los consumers funcionaron correctamente de extremo a extremo. Los documentos insertados en Elasticsearch presentaron el formato JSON esperado, con el campo `@timestamp` correctamente tipado como `date`, lo que permitio a Kibana aplicar filtros temporales y ordenar los datos cronologicamente.

El indice `gittba_BTC` almacena los datos raw de cotizacion con cada cierre de vela de 1 minuto. El indice `gittba_BTC_VWAP` almacena los resultados del calculo VWAP por ventanas de 5 minutos, con el campo `@timestamp` derivado del final de cada ventana temporal.

El dashboard de Kibana permite visualizar en tiempo real la evolucion del precio de cierre, el volumen negociado y el indicador VWAP, cumpliendo con el objetivo de la HU-8.

---

## Conclusion

En este sprint se ha completado la arquitectura de procesamiento streaming cerrando el ciclo con la capa de almacenamiento indexado y visualizacion. Los consumers desarrollados actuan como puente entre Apache Kafka y Elasticsearch, consumiendo los mensajes de ambos topics (datos raw y VWAP) e insertandolos como documentos JSON con el campo temporal requerido.

La creacion automatica de indices con mapping explicito garantiza que Elasticsearch interprete correctamente los tipos de datos, evitando problemas de inferencia automatica que podrian afectar a las consultas y visualizaciones. El uso de la libreria `requests` proporciona control directo sobre las llamadas HTTP, facilitando la comprension del protocolo REST de Elasticsearch.

El dashboard configurado en Kibana permite a cualquier miembro del equipo consultar y explorar los datos de cotizacion y VWAP en tiempo real, sin necesidad de escribir codigo ni acceder directamente a Kafka o Elasticsearch. Con esto, el pipeline completo queda operativo de extremo a extremo: Binance WebSocket -> Kafka Producer -> Kafka Topics -> Spark Streaming (VWAP) -> Kafka Topics -> Python Consumers -> Elasticsearch -> Kibana.

---

## Anexo

Enlace a GitHub: https://github.com/GonAlonsoLid/trading-view-big-data

### Instrucciones de ejecucion

**Requisitos previos:**
```bash
pip3 install kafka-python requests
```

**1. Lanzar el consumer de datos raw BTC -> Elasticsearch:**
```bash
python pipelines/spark/elastic_consumer_btc.py
```

**2. Lanzar el consumer de datos VWAP -> Elasticsearch:**
```bash
python pipelines/spark/elastic_consumer_vwap_btc.py
```

**3. Configurar Kibana:**
- Acceder a http://192.168.80.37:5601
- Management -> Stack Management -> Kibana -> Data Views
- Crear Data View con index pattern `gittba_BTC*` y timestamp field `@timestamp`
- Crear Data View con index pattern `gittba_BTC_VWAP*` y timestamp field `@timestamp`
- Analytics -> Dashboard -> Create dashboard -> Create visualization

### Comandos utiles de Elasticsearch

```bash
# Comprobar estado del cluster
curl -X GET http://192.168.80.37:9201/_cat/health?v -u user:password

# Listar indices
curl -X GET http://192.168.80.37:9201/_cat/indices?v -u user:password

# Ver mapping de un indice
curl -X GET http://192.168.80.37:9201/gittba_BTC?pretty -u user:password

# Buscar documentos en un indice
curl -X GET 'http://192.168.80.37:9201/gittba_BTC/_search?pretty&size=5' -u user:password

# Borrar un indice (si se necesita recrear)
curl -X DELETE http://192.168.80.37:9201/gittba_BTC -u user:password
```
