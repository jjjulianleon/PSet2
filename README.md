# PSet 02 — Data Mining (USFQ)

**Estudiante:** Julian Leon
**Codigo banner:** 00329141

**NYC TLC Trip Record Data Pipeline** — End-to-end data pipeline orquestado con Mage, PostgreSQL y dbt.

---

## 1. Arquitectura (Medallion: Bronze → Silver → Gold)

```
NYC TLC (Parquet) --> [BRONZE] --> [SILVER] --> [GOLD]
  Yellow + Green       raw         views       tablas
  2022-01 a 2025-11    bronze.*    silver.*    gold.*
```

**Flujo de datos:**

| Capa | Esquema | Tipo | Contenido |
|------|---------|------|-----------|
| Bronze | `bronze.trips` | Tabla raw | Datos crudos + metadatos (`ingest_ts`, `source_month`, `service_type`) |
| Silver | `silver.stg_trips` | Vista (dbt) | Limpieza: nulos, rangos, tipificacion, taxi zones |
| Gold | `gold.fct_trips` | Tabla particionada (RANGE) | Hechos: 1 fila = 1 viaje |
| Gold | `gold.dim_date` | Tabla | Dimension fecha |
| Gold | `gold.dim_vendor` | Tabla | Dimension proveedor |
| Gold | `gold.dim_zone` | Tabla particionada (HASH x4) | Dimension zona |
| Gold | `gold.dim_service_type` | Tabla particionada (LIST) | yellow / green |
| Gold | `gold.dim_payment_type` | Tabla particionada (LIST) | 6 tipos de pago |

**Orquestacion (Mage):**

```
ingest_bronze --> (API trigger) --> dbt_after_ingest
                                      1. dbt run --select silver
                                      2. SQL particionamiento
                                      3. dbt run --select gold
                                      4. dbt test
```

**Materializaciones:**
- **Silver**: `view` (stg_trips)
- **Gold**: `table` (dim_date, dim_vendor) + tablas particionadas via SQL (fct_trips, dim_zone, dim_service_type, dim_payment_type)

---

## 2. Tabla de cobertura por mes y servicio

94 archivos ingestados (47 yellow + 47 green), desde 2022-01 hasta 2025-11.

| year_month | service_type | status | row_count |
|------------|-------------|--------|-----------|
| 2022-01 | green | loaded | 62,495 |
| 2022-01 | yellow | loaded | 2,463,931 |
| 2022-02 | green | loaded | 69,399 |
| 2022-02 | yellow | loaded | 2,979,431 |
| 2022-03 | green | loaded | 78,537 |
| 2022-03 | yellow | loaded | 3,627,882 |
| 2022-04 | green | loaded | 76,136 |
| 2022-04 | yellow | loaded | 3,599,920 |
| 2022-05 | green | loaded | 76,891 |
| 2022-05 | yellow | loaded | 3,588,295 |
| 2022-06 | green | loaded | 73,718 |
| 2022-06 | yellow | loaded | 3,558,124 |
| 2022-07 | green | loaded | 64,192 |
| 2022-07 | yellow | loaded | 3,174,394 |
| 2022-08 | green | loaded | 65,929 |
| 2022-08 | yellow | loaded | 3,152,677 |
| 2022-09 | green | loaded | 69,031 |
| 2022-09 | yellow | loaded | 3,183,767 |
| 2022-10 | green | loaded | 69,322 |
| 2022-10 | yellow | loaded | 3,675,411 |
| 2022-11 | green | loaded | 62,313 |
| 2022-11 | yellow | loaded | 3,252,717 |
| 2022-12 | green | loaded | 72,439 |
| 2022-12 | yellow | loaded | 3,399,549 |
| 2023-01 | green | loaded | 68,211 |
| 2023-01 | yellow | loaded | 3,066,766 |
| 2023-02 | green | loaded | 64,809 |
| 2023-02 | yellow | loaded | 2,913,955 |
| 2023-03 | green | loaded | 72,044 |
| 2023-03 | yellow | loaded | 3,403,766 |
| 2023-04 | green | loaded | 65,392 |
| 2023-04 | yellow | loaded | 3,288,250 |
| 2023-05 | green | loaded | 69,174 |
| 2023-05 | yellow | loaded | 3,513,649 |
| 2023-06 | green | loaded | 65,550 |
| 2023-06 | yellow | loaded | 3,307,234 |
| 2023-07 | green | loaded | 61,343 |
| 2023-07 | yellow | loaded | 2,907,108 |
| 2023-08 | green | loaded | 60,649 |
| 2023-08 | yellow | loaded | 2,824,209 |
| 2023-09 | green | loaded | 65,471 |
| 2023-09 | yellow | loaded | 2,846,722 |
| 2023-10 | green | loaded | 66,177 |
| 2023-10 | yellow | loaded | 3,522,285 |
| 2023-11 | green | loaded | 64,025 |
| 2023-11 | yellow | loaded | 3,339,715 |
| 2023-12 | green | loaded | 64,215 |
| 2023-12 | yellow | loaded | 3,376,567 |
| 2024-01 | green | loaded | 56,551 |
| 2024-01 | yellow | loaded | 2,964,624 |
| 2024-02 | green | loaded | 53,577 |
| 2024-02 | yellow | loaded | 3,007,526 |
| 2024-03 | green | loaded | 57,457 |
| 2024-03 | yellow | loaded | 3,582,628 |
| 2024-04 | green | loaded | 56,471 |
| 2024-04 | yellow | loaded | 3,514,289 |
| 2024-05 | green | loaded | 61,003 |
| 2024-05 | yellow | loaded | 3,723,833 |
| 2024-06 | green | loaded | 54,748 |
| 2024-06 | yellow | loaded | 3,539,193 |
| 2024-07 | green | loaded | 51,837 |
| 2024-07 | yellow | loaded | 3,076,903 |
| 2024-08 | green | loaded | 51,771 |
| 2024-08 | yellow | loaded | 2,979,183 |
| 2024-09 | green | loaded | 54,440 |
| 2024-09 | yellow | loaded | 3,633,030 |
| 2024-10 | green | loaded | 56,147 |
| 2024-10 | yellow | loaded | 3,833,771 |
| 2024-11 | green | loaded | 52,222 |
| 2024-11 | yellow | loaded | 3,646,369 |
| 2024-12 | green | loaded | 53,994 |
| 2024-12 | yellow | loaded | 3,668,371 |
| 2025-01 | green | loaded | 48,326 |
| 2025-01 | yellow | loaded | 3,475,226 |
| 2025-02 | green | loaded | 46,621 |
| 2025-02 | yellow | loaded | 3,577,543 |
| 2025-03 | green | loaded | 51,539 |
| 2025-03 | yellow | loaded | 4,145,257 |
| 2025-04 | green | loaded | 52,132 |
| 2025-04 | yellow | loaded | 3,970,553 |
| 2025-05 | green | loaded | 55,399 |
| 2025-05 | yellow | loaded | 4,591,845 |
| 2025-06 | green | loaded | 49,390 |
| 2025-06 | yellow | loaded | 4,322,960 |
| 2025-07 | green | loaded | 48,205 |
| 2025-07 | yellow | loaded | 3,898,963 |
| 2025-08 | green | loaded | 46,306 |
| 2025-08 | yellow | loaded | 3,574,091 |
| 2025-09 | green | loaded | 48,893 |
| 2025-09 | yellow | loaded | 4,251,015 |
| 2025-10 | green | loaded | 49,416 |
| 2025-10 | yellow | loaded | 4,428,699 |
| 2025-11 | green | loaded | 46,912 |
| 2025-11 | yellow | loaded | 4,181,444 |

**Nota:** 2025-12 no estaba disponible en la pagina oficial al momento de la ingesta.

---

## 3. Como levantar el stack

```bash
# 1. Clonar el repositorio
git clone https://github.com/jjjulianleon/PSet2.git
cd PSet2

# 2. Crear .env a partir de .env.example
cp .env.example .env
# Editar .env con los valores reales:
#   POSTGRES_USER=admin
#   POSTGRES_PASSWORD=<tu_password>
#   POSTGRES_DB=nyc_tlc
#   POSTGRES_PORT=5432
#   MAGE_PORT=6789

# 3. Levantar los contenedores
docker compose up -d

# 4. Acceder a Mage en http://localhost:6789
```

**Requisitos:** Docker + Docker Compose instalados. Se recomienda al menos 10 GB de RAM disponibles.

---

## 4. Pipelines de Mage

| Pipeline | Bloques | Descripcion |
|----------|---------|-------------|
| `ingest_bronze` | `ingest_bronze_custom` → `trigger_dbt_chain` | Descarga parquets de NYC TLC (Yellow+Green), inserta en `bronze.trips` con metadatos (`ingest_ts`, `source_month`, `service_type`). Idempotente: DELETE antes de INSERT por mes/servicio. Al finalizar, dispara `dbt_after_ingest` via API. |
| `dbt_build_silver` | `run_dbt_silver` | Ejecuta `dbt run --select silver` (crea vista `silver.stg_trips`). |
| `dbt_build_gold` | `run_partitioning` → `run_dbt_gold` | Ejecuta scripts SQL de particionamiento + `dbt run --select gold`. |
| `quality_checks` | `run_dbt_test` | Ejecuta `dbt test` (12 tests). Falla si algun test no pasa. |
| `dbt_after_ingest` | `run_dbt_silver_chain` → `run_partitioning_chain` → `run_dbt_gold_chain` → `run_dbt_test_chain` | Pipeline encadenado que ejecuta todo el flujo dbt en orden (silver → particiones → gold → tests). |

---

## 5. Triggers

| Trigger | Tipo | Pipeline | Frecuencia |
|---------|------|----------|------------|
| `ingest_monthly` | Schedule | `ingest_bronze` | Semanal |
| `dbt_after_ingest` | API | `dbt_after_ingest` | Disparado por `ingest_bronze` al terminar |

**Nota:** Mage v0.9.79 no soporta Event triggers nativos. Se implemento pipeline chaining via API trigger: el ultimo bloque de `ingest_bronze` hace un POST HTTP al endpoint API de `dbt_after_ingest`.

---

## 6. Gestion de secretos (Mage Secrets)

Todas las credenciales de PostgreSQL se manejan via **Mage Secrets** y variables de entorno `.env`.

| Secret | Proposito |
|--------|-----------|
| `POSTGRES_HOST` | Host del servidor PostgreSQL |
| `POSTGRES_PORT` | Puerto de conexion (5432) |
| `POSTGRES_USER` | Usuario de la base de datos |
| `POSTGRES_PASSWORD` | Contrasena de la base de datos |
| `POSTGRES_DB` | Nombre de la base de datos |

- `.env` esta en `.gitignore` (nunca se sube al repo)
- `.env.example` esta versionado con las variables sin valores
- `profiles.yml` de dbt usa `env_var()` para leer credenciales
- No hay credenciales hardcodeadas en el codigo

---

## 7. Particionamiento declarativo

### 7.1 RANGE — `gold.fct_trips`

Particionada por `pickup_date` con 48 particiones mensuales (2022-01 a 2025-12) + 1 DEFAULT.

```
\d+ gold.fct_trips
Partition key: RANGE (pickup_date)
Partitions: gold.fct_trips_2022_01 FOR VALUES FROM ('2022-01-01') TO ('2022-02-01'),
            gold.fct_trips_2022_02 FOR VALUES FROM ('2022-02-01') TO ('2022-03-01'),
            ...
            gold.fct_trips_2025_12 FOR VALUES FROM ('2025-12-01') TO ('2026-01-01'),
            gold.fct_trips_default DEFAULT
```

### 7.2 HASH — `gold.dim_zone`

Particionada por `zone_key` con 4 particiones.

```
\d+ gold.dim_zone
Partition key: HASH (zone_key)
Partitions: gold.dim_zone_p0 FOR VALUES WITH (modulus 4, remainder 0),
            gold.dim_zone_p1 FOR VALUES WITH (modulus 4, remainder 1),
            gold.dim_zone_p2 FOR VALUES WITH (modulus 4, remainder 2),
            gold.dim_zone_p3 FOR VALUES WITH (modulus 4, remainder 3)
```

### 7.3 LIST — `gold.dim_service_type`

Particionada por `service_type` con 2 particiones.

```
\d+ gold.dim_service_type
Partition key: LIST (service_type)
Partitions: gold.dim_service_type_green FOR VALUES IN ('green'),
            gold.dim_service_type_yellow FOR VALUES IN ('yellow')
```

### 7.4 LIST — `gold.dim_payment_type`

Particionada por `payment_type` con 6 particiones.

```
\d+ gold.dim_payment_type
Partition key: LIST (payment_type)
Partitions: gold.dim_payment_type_cash FOR VALUES IN ('Cash'),
            gold.dim_payment_type_credit FOR VALUES IN ('Credit card'),
            gold.dim_payment_type_dispute FOR VALUES IN ('Dispute'),
            gold.dim_payment_type_no_charge FOR VALUES IN ('No charge'),
            gold.dim_payment_type_unknown FOR VALUES IN ('Unknown'),
            gold.dim_payment_type_voided FOR VALUES IN ('Voided trip')
```

### 7.5 Evidencia de partition pruning

**Consulta 1 — RANGE pruning en `fct_trips` (filtro por mes):**

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM gold.fct_trips
WHERE pickup_date >= '2024-03-01' AND pickup_date < '2024-04-01'
LIMIT 5;
```

```
Limit  (cost=0.00..0.16 rows=5 width=96) (actual time=0.220..0.222 rows=5 loops=1)
  Buffers: shared read=1
  ->  Seq Scan on fct_trips_2024_03 fct_trips  (cost=0.00..112871.99 rows=3595466 width=96)
        Filter: ((pickup_date >= '2024-03-01'::date) AND (pickup_date < '2024-04-01'::date))
        Buffers: shared read=1
Planning Time: 1.948 ms
Execution Time: 0.266 ms
```

PostgreSQL aplica **partition pruning**: solo escanea `fct_trips_2024_03` en lugar de las 49 particiones. El filtro por fecha hace que el planner descarte todas las demas particiones, reduciendo drasticamente el I/O.

**Consulta 2 — HASH pruning en `dim_zone` (busqueda por zone_key):**

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM gold.dim_zone WHERE zone_key = 132;
```

```
Seq Scan on dim_zone_p3 dim_zone  (cost=0.00..17.88 rows=3 width=100) (actual time=0.011..0.014 rows=1 loops=1)
  Filter: (zone_key = 132)
  Rows Removed by Filter: 48
  Buffers: shared hit=1
Planning Time: 0.358 ms
Execution Time: 0.039 ms
```

PostgreSQL aplica **partition pruning**: con `zone_key = 132` solo accede a `dim_zone_p3` (la particion correspondiente al hash de 132 mod 4). Las otras 3 particiones son descartadas por el planner.

---

## 8. dbt — Materializaciones y tests

### Materializaciones

| Capa | Modelo | Materializacion |
|------|--------|-----------------|
| Silver | `stg_trips` | `view` |
| Gold | `dim_date` | `table` |
| Gold | `dim_vendor` | `table` |
| Gold | `fct_trips`, `dim_zone`, `dim_service_type`, `dim_payment_type` | Tablas particionadas via SQL |

### Reglas de limpieza en Silver (`stg_trips`)

- `pickup_datetime IS NOT NULL`
- `dropoff_datetime IS NOT NULL`
- `pickup_datetime <= dropoff_datetime`
- `trip_distance >= 0`
- `total_amount >= 0`
- `PULocationID BETWEEN 1 AND 263` (zonas validas)
- `DOLocationID BETWEEN 1 AND 263` (zonas validas)
- Rango temporal: `2022-01-01` a `2026-01-01`

### dbt tests (13 total, todos passing)

Tests configurados en `schema.yml` y `sources_gold.yml`:

- `unique` + `not_null` en: `fct_trips.trip_key`, `dim_zone.zone_key`, `dim_date.date_key`, `dim_vendor.vendor_key`
- `relationships`:
  - `fct_trips.pu_zone_key` → `dim_zone.zone_key`
  - `fct_trips.do_zone_key` → `dim_zone.zone_key`
  - `fct_trips.pickup_date_key` → `dim_date.date_key`
- `accepted_values`:
  - `dim_service_type.service_type` in `('yellow', 'green')`
  - `dim_payment_type.payment_type` in `('Credit card', 'Cash', 'No charge', 'Dispute', 'Unknown', 'Voided trip')`

```
dbt test output:
  13 of 13 PASS — All tests passed
```

---

## 9. Troubleshooting

### Problema 1: `SyntaxError: engine =` al ejecutar particionamiento en Mage

**Causa:** Al copiar codigo en el editor de Mage, una linea larga se corto por un salto de linea involuntario, separando `engine =` de `create_engine(conn_str)`.

**Solucion:** Refactorizar la linea larga en dos variables separadas (`conn_str` y `engine`) para evitar el corte.

### Problema 2: `AttributeError: 'Connection' object has no attribute 'commit'`

**Causa:** La version de SQLAlchemy instalada en el contenedor de Mage no soporta `conn.commit()` en objetos Connection obtenidos con `engine.connect()`.

**Solucion:** Cambiar de `engine.connect()` + `conn.commit()` a `engine.begin()`, que maneja el commit automaticamente al salir del bloque `with`.

### Problema 3: `ProgrammingError: function round(double precision, integer) does not exist`

**Causa:** PostgreSQL no permite aplicar `ROUND()` directamente sobre valores `double precision`. La funcion solo acepta `numeric`.

**Solucion:** Agregar un cast explicito `::numeric` antes de aplicar ROUND: `ROUND((AVG(...) * 100)::numeric, 2)`.

### Problema 4: Mage no tiene Event triggers (solo Schedule y API)

**Causa:** Mage v0.9.79 no soporta Event triggers nativos para encadenar pipelines automaticamente.

**Solucion:** Se creo un pipeline `dbt_after_ingest` con un API trigger, y se agrego un bloque al final de `ingest_bronze` que hace un POST HTTP al endpoint API para disparar la cadena.

---

## 10. Estructura del repositorio

```
PSet2/
├── docker-compose.yml          # Stack: PostgreSQL 15 + Mage
├── .env.example                # Template de variables de entorno
├── .gitignore                  # Excluye .env, parquets, cache
├── README.md                   # Este archivo
├── DM-PSet-2.pdf               # Enunciado del PSet
├── sql/
│   ├── init/
│   │   └── 01_create_schemas.sql   # CREATE SCHEMA bronze/silver/gold
│   └── partitioning/
│       └── 01_create_partitions.sql # Particiones declarativas
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml            # Usa env_var() para credenciales
│   ├── models/
│   │   ├── silver/
│   │   │   └── stg_trips.sql   # Vista con limpieza
│   │   └── gold/
│   │       ├── dim_date.sql
│   │       ├── dim_vendor.sql
│   │       ├── schema.yml      # Tests de dim_date, dim_vendor
│   │       └── sources_gold.yml # Tests de tablas particionadas
│   └── macros/
├── mage_project/
│   ├── custom/
│   │   ├── ingest_bronze_custom.py
│   │   ├── trigger_dbt_chain.py
│   │   ├── run_dbt_silver.py
│   │   ├── run_dbt_silver_chain.py
│   │   ├── run_dbt_gold.py
│   │   ├── run_dbt_gold_chain.py
│   │   ├── run_partitioning.py
│   │   ├── run_partitioning_chain.py
│   │   ├── run_dbt_test.py
│   │   └── run_dbt_test_chain.py
│   └── pipelines/
│       ├── ingest_bronze/
│       ├── dbt_build_silver/
│       ├── dbt_build_gold/
│       ├── quality_checks/
│       └── dbt_after_ingest/
├── notebooks/
│   └── NotebookPSet2.ipynb     # 20 preguntas de negocio (solo gold.*)
└── capturas/                   # Screenshots de evidencia
```

---

## 11. Notebook de analisis

El notebook `notebooks/NotebookPSet2.ipynb` contiene 20 preguntas de negocio respondidas **exclusivamente con tablas `gold.*`**:

1. Viajes por mes (2024)
2. Viajes por service_type y mes
3. Top 10 zonas de pickup
4. Top 10 zonas de dropoff
5. Top 5 boroughs por mes (pickup)
6. Horas pico por dia de semana
7. Distribucion por dia de semana
8. Ingreso total por mes
9. Ingreso por service_type y mes
10. Tip % promedio por mes
11. Tip % por borough y mes
12. Top 10 zonas por ingreso total
13. Top 10 zonas por tip % (min 1000 viajes)
14. Cash vs Card (viajes, ingreso, tip %)
15. Duracion promedio por mes
16. Distancia promedio por mes
17. Velocidad promedio por borough y franja horaria
18. Percentiles p50/p90 de duracion por borough
19. Top 10 zonas por p90 de duracion
20. Top 10 rutas borough→borough

---

## 12. Checklist de aceptacion

- [x] Docker Compose levanta Postgres + Mage
- [x] Credenciales en Mage Secrets y .env (solo .env.example en repo)
- [x] Pipeline ingest_bronze mensual e idempotente + tabla de cobertura
- [x] dbt corre dentro de Mage: dbt_build_silver, dbt_build_gold, quality_checks
- [x] Silver materialized = views; Gold materialized = tables
- [x] Gold tiene esquema estrella completo
- [x] Particionamiento: RANGE en fct_trips, HASH en dim_zone, LIST en dim_service_type y dim_payment_type
- [x] README incluye \d+ y EXPLAIN (ANALYZE, BUFFERS) con pruning
- [x] dbt test pasa desde Mage (13/13 PASS)
- [x] Notebook responde 20 preguntas usando solo gold.*
- [x] Triggers configurados y evidenciados
