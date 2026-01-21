# Trading View Big Data Platform

Plataforma escalable para ingestión, procesamiento y visualización de datos de trading de criptomonedas.

## 🎯 Estado Actual

**Iteración 1**: Ingesta batch histórica de datos OHLCV desde Binance.

| Componente | Estado |
|------------|--------|
| Ingesta Batch OHLCV | ✅ Implementado |
| Cliente Binance | ✅ Implementado |
| Almacenamiento Parquet | ✅ Implementado |
| CLI de Backfill | ✅ Implementado |
| Streaming (Kafka) | 🔜 Planificado |
| Spark Jobs | 🔜 Planificado |
| API REST | 🔜 Planificado |
| Frontend | 🔜 Planificado |

## 📁 Estructura del Proyecto

```
trading-view-big-data/
├── apps/                      # Aplicaciones ejecutables
│   └── ingest_batch/          # CLI para ingesta batch
├── packages/                  # Librerías reutilizables
│   ├── clients/               # Clientes de APIs externas
│   │   └── binance/           # Cliente Binance (klines)
│   ├── contracts/             # Contratos de datos
│   │   └── ohlcv.py           # Schema OHLCV v1
│   ├── storage/               # Backends de almacenamiento
│   │   └── parquet_writer.py  # Escritor Parquet particionado
│   └── utils/                 # Utilidades comunes
├── pipelines/                 # Pipelines de datos
│   ├── batch/                 # Pipelines batch (backfill)
│   ├── streaming/             # (Futuro) Kafka consumers
│   └── spark/                 # (Futuro) Jobs Spark
├── infra/                     # Infraestructura
│   ├── docker-compose.yml     # (Placeholder)
│   └── configs/               # Configuraciones
├── docs/                      # Documentación
│   ├── contracts/             # Contratos de datos
│   └── decisions/             # ADRs (Architecture Decision Records)
├── data_lake/                 # Almacenamiento local (no versionado)
└── tests/                     # Tests
```

## 🚀 Instalación

### Requisitos

- Python 3.11+
- pip o poetry

### Setup

```bash
# Clonar repositorio
git clone <repo-url>
cd trading-view-big-data

# Crear entorno virtual
python -m venv venv

# Activar entorno (Windows)
.\venv\Scripts\activate

# Activar entorno (Linux/Mac)
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt

# (Opcional) Instalar en modo desarrollo
pip install -e ".[dev]"
```

## 📊 Uso: Backfill de Datos OHLCV

### Comando Básico

```bash
# Backfill por defecto: BTCUSDT, 1h, 2022-01-01 a 2025-12-31
python -m apps.ingest_batch backfill
```

### Opciones Disponibles

```bash
python -m apps.ingest_batch backfill --help

Options:
  --symbol TEXT               Trading pair symbol (default: BTCUSDT)
  --timeframe [1h|1d|all]     Timeframe: 1h, 1d, or all (default: 1h)
  --start TEXT                Start date YYYY-MM-DD (default: 2022-01-01)
  --end TEXT                  End date YYYY-MM-DD (default: 2025-12-31)
  --out PATH                  Output path (default: data_lake)
  --max-rows-per-file INT     Max rows per file (default: 200000)
  --overwrite                 Overwrite existing files
  --no-progress               Disable progress bar
  --log-level [DEBUG|INFO|WARNING|ERROR]
```

### Ejemplos

```bash
# Solo timeframe 1h (por defecto)
python -m apps.ingest_batch backfill --timeframe 1h

# Solo timeframe 1d
python -m apps.ingest_batch backfill --timeframe 1d

# Ambos timeframes (1h y 1d)
python -m apps.ingest_batch backfill --timeframe all

# Rango de fechas personalizado
python -m apps.ingest_batch backfill --start 2023-01-01 --end 2023-12-31

# Con logging detallado
python -m apps.ingest_batch --log-level DEBUG backfill

# Sobrescribir archivos existentes
python -m apps.ingest_batch backfill --overwrite
```

### Ver Información de Datos Existentes

```bash
python -m apps.ingest_batch info --timeframe 1h
```

## 📂 Particionado y Estructura de Archivos

Los datos se almacenan en formato Parquet con particionado jerárquico:

```
data_lake/raw/ohlcv/
└── source=binance/
    └── symbol=BTCUSDT/
        └── timeframe=1h/
            ├── year=2022/
            │   ├── month=01/
            │   │   └── part-00000.parquet
            │   ├── month=02/
            │   │   └── part-00000.parquet
            │   └── ...
            ├── year=2023/
            └── ...
```

### Beneficios del Particionado

1. **Consultas eficientes**: Filtrado por fecha sin escanear todo
2. **Control de tamaño**: Archivos pequeños y manejables
3. **Procesamiento paralelo**: Particiones independientes
4. **Gestión del ciclo de vida**: Fácil eliminación de datos antiguos

### Split Automático

Si una partición mensual excede el umbral de filas (default: 200,000), se divide automáticamente:

```
year=2023/month=01/
├── part-00000.parquet  (200,000 rows)
├── part-00001.parquet  (200,000 rows)
└── part-00002.parquet  (remaining rows)
```

## 📋 Contrato de Datos OHLCV v1

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `event_time` | timestamp (UTC) | Tiempo de apertura de la vela |
| `symbol` | string | Par de trading (ej: BTCUSDT) |
| `timeframe` | string | Intervalo (ej: 1h, 1d) |
| `open` | float | Precio de apertura |
| `high` | float | Precio máximo |
| `low` | float | Precio mínimo |
| `close` | float | Precio de cierre |
| `volume` | float | Volumen en activo base |
| `source` | string | Fuente de datos (binance) |
| `ingestion_time` | timestamp (UTC) | Momento de ingestión |

### Validaciones

- `high >= max(open, close)`
- `low <= min(open, close)`
- `volume >= 0`
- Sin valores nulos en campos requeridos

## 🧪 Tests

```bash
# Ejecutar todos los tests
pytest

# Con verbose
pytest -v

# Solo smoke test de ingesta
pytest tests/test_batch_ingestion.py::TestBackfillPipeline -v
```

## 🔮 Extensibilidad Futura

### Añadir Más Símbolos

El diseño actual soporta múltiples símbolos de forma nativa:

```bash
# Ejemplo futuro (mismo código, diferente símbolo)
python -m apps.ingest_batch backfill --symbol ETHUSDT
python -m apps.ingest_batch backfill --symbol SOLUSDT
```

Los datos se almacenarán en particiones separadas:
```
data_lake/raw/ohlcv/source=binance/
├── symbol=BTCUSDT/
├── symbol=ETHUSDT/
└── symbol=SOLUSDT/
```

### Añadir Más Fuentes de Datos

Para añadir una nueva fuente (ej: Coinbase):

1. **Crear cliente** en `packages/clients/coinbase/`
2. **Mapear datos** al contrato OHLCV existente
3. **Usar el mismo pipeline** cambiando `source="coinbase"`

```
data_lake/raw/ohlcv/
├── source=binance/
└── source=coinbase/
```

### Añadir Timeframes

El cliente Binance ya soporta múltiples timeframes:
- 1m, 3m, 5m, 15m, 30m (minutos)
- 1h, 2h, 4h, 6h, 8h, 12h (horas)
- 1d, 3d, 1w, 1M (días/semanas/meses)

### Próximas Iteraciones Planificadas

1. **Streaming**: Kafka producers/consumers para datos en tiempo real
2. **Spark Jobs**: Cálculo de indicadores técnicos (SMA, EMA, RSI, MACD)
3. **API REST**: FastAPI para servir datos
4. **WebSocket Server**: Updates en tiempo real
5. **Frontend**: Dashboard tipo TradingView con gráficos interactivos

## 📚 Documentación Adicional

- [Contrato OHLCV v1](docs/contracts/ohlcv_v1.md)
- [ADR-001: Arquitectura Inicial](docs/decisions/adr-001-initial-architecture.md)

## 🛠️ Desarrollo

### Estructura de Código

- **packages/**: Código reutilizable sin dependencias de aplicación
- **pipelines/**: Orquestación de workflows de datos
- **apps/**: Puntos de entrada (CLI, servidores)

### Convenciones

- Símbolos: MAYÚSCULAS sin separador (`BTCUSDT`)
- Timeframes: minúsculas con unidad (`1h`, `1d`)
- Sources: minúsculas (`binance`, `coinbase`)
- Timestamps: UTC siempre

## 📄 Licencia

MIT
