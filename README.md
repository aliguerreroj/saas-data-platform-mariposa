# SaaS Data Platform — Grupo Mariposa

Pipeline de datos multi-tenant (PySpark + Delta Lake) con arquitectura medallion
(Bronze → Silver → Gold), orquestado por un CLI que corre un tenant a la vez o
todos los tenants conocidos en una sola invocación.

## Arquitectura

- **Bronze**: ingesta cruda desde CSV, todas las columnas de negocio como
  `String` a propósito (ningún valor original se pierde en el parseo). Idempotente
  vía overwrite acotado a la partición (`fecha_proceso`, `_tenant_id`) que trae
  cada corrida — no toca las particiones de otros rangos de fecha.
- **Silver**:
  - `fact_deliveries`: dedupe exacto → filtro de `tipo_entrega` → clasificación
    de anomalías (cuarentena) → normalización de unidades → enriquecido con
    `dim_materials` vía **join temporal** (no por `is_current`) → `MERGE INTO`
    por clave compuesta (`_tenant_id, fecha_proceso, transporte, ruta, material,
    tipo_entrega`).
  - `dim_materials`: SCD Type 2. `is_current` se **recalcula** a partir de cuál
    fila tiene `valid_to = 9999-12-31` — nunca se confía en el flag que trae el
    archivo de origen.
- **Gold**: `daily_metrics_by_delivery_type`, agregación derivada (no
  autoritativa) a partir de `fact_deliveries`. Se recomputa por partición de
  fecha en cada corrida (`recompute_partition`), acotado a las particiones que
  trae el batch actual.
- **Quality**: 5 checks (fechas, cantidades/precios, duplicados, cobertura del
  catálogo, un solo `is_current` por SKU) corridos sobre lo que Silver acaba de
  producir. Los resultados se guardan en `quality_logs`, una tabla Delta
  compartida entre tenants. Si `quality.fail_on_critical=true` y falla un check
  `critical`, el tenant aborta antes de escribir Gold (los demás tenants, en una
  corrida `--tenant all`, siguen).

Aislamiento multi-tenant: por *path* en cada capa (`data/<capa>/<tenant>/<tabla>`),
no por permisos — ver la sección "Qué dejé fuera y por qué".

## Estructura del repo
.
├── .github/workflows/ci.yml # CI: lint + validación de configs + tests
├── config/
│ ├── base.yaml # capa base (reglas de negocio, paths, defaults)
│ ├── env/{dev,qa,main}.yaml # overrides por ambiente (paths, fail_on_critical)
│ └── tenants/{gt,sv,hn,jm,pe,ec}.yaml # un archivo = un tenant conocido
├── raw/
│ ├── global_mobility_data_entrega_productos.csv # entregas de TODOS los tenants
│ │ # (se filtra por la columna pais)
│ └── materials_catalog.csv # catálogo de materiales, compartido entre tenants
├── scripts/
│ └── validate_configs.py # valida que TODAS las combinaciones (env, tenant) carguen
├── src/saas_pipeline/
│ ├── cli.py # orquestador: un tenant o --tenant all
│ ├── config.py # carga y merge de config (base -> env -> tenant)
│ ├── schemas.py # esquemas RAW -> Bronze
│ ├── bronze.py # ingesta cruda
│ ├── silver.py # limpieza, cuarentena, SCD2, enriquecimiento
│ ├── gold.py # agregación de negocio
│ ├── quality.py # 5 checks de calidad sobre Silver
│ ├── delta_io.py # helpers de escritura Delta (merge, overwrite por partición)
│ └── spark_session.py # construcción de la SparkSession (+ fixes de Windows)
├── tests/
│ ├── conftest.py # fixture spark (SparkSession sin Delta, session-scoped)
│ └── test_transformations.py # 10 tests de transformaciones puras
├── docs/
│ ├── observations.md # hallazgos y decisiones técnicas
│ ├── onboarding-tenant.md # cómo agregar un tenant nuevo
│ └── infra.md # módulo Terraform ilustrativo
├── mentoring/ # ejercicio de code review (para un dev junior)
├── requirements.txt
└── pyproject.toml # config de pytest y ruff




## Requisitos

- Python 3.11+ (CI corre en 3.11; también probado en 3.13 vía Anaconda en Windows).
- Java 17 (Temurin recomendado). El código detecta la versión de Java instalada
  y aplica los flags `--add-opens` que Spark/Delta 3.5.x necesitan en Java 17+;
  con Java 8/11 no hacen falta y no se aplican.
- **Solo Windows**: `winutils.exe` + `hadoop.dll` compatibles con Hadoop 3.3.x
  (la versión que trae pyspark 3.5.3), en una carpeta con la variable de entorno
  `HADOOP_HOME` apuntando ahí y `%HADOOP_HOME%\bin` agregado al `PATH`. Sin esto,
  las operaciones de escritura de Hadoop/Delta fallan en Windows.

## Instalación

```bash
pip install -r requirements.txt
```

## Cómo correr el pipeline

Un tenant, un rango de fechas, ambiente `dev` (default):
```bash
python -m saas_pipeline.cli --tenant gt --start-date 2025-01-01 --end-date 2025-12-31
```

Todos los tenants conocidos (uno por archivo en `config/tenants/`):
```bash
python -m saas_pipeline.cli --tenant all --start-date 2025-01-01 --end-date 2025-12-31
```

Otro ambiente (paths y `fail_on_critical` distintos — ver `config/env/`):
```bash
python -m saas_pipeline.cli --tenant gt --env qa --start-date 2025-01-01 --end-date 2025-12-31
```

Flags opcionales:
- `--fail-fast` / `--no-fail-fast`: en `--tenant all`, si un tenant falla, aborta
  el resto (`--fail-fast`) o sigue y reporta al final (`--no-fail-fast`). Sin el
  flag, usa `execution.fail_fast` del ambiente (`false` por default).
- `--fail-on-critical` / `--no-fail-on-critical`: si un quality check `critical`
  falla, aborta ANTES de escribir Gold para ese tenant. Sin el flag, usa
  `quality.fail_on_critical` del ambiente (`false` en dev, `true` en qa/main).

## Cómo correr los tests y el linter

```bash
pytest -v
ruff check src tests scripts
python scripts/validate_configs.py
```

`tests/test_transformations.py` prueba las transformaciones puras de
bronze/silver/gold/quality con DataFrames sintéticos chiquitos — no lee los CSV
reales ni escribe a Delta, así que corre rápido y no depende de que Maven
resuelva `io.delta:delta-spark` (por eso `conftest.py` usa `get_plain_spark()`,
no `get_spark()`).

**Nota Windows**: si `pytest` falla con *"Python worker exited unexpectedly
(crashed)"*, es casi siempre porque algo construyó un DataFrame con
`spark.createDataFrame(lista_de_python, schema)` — ese patrón crea un
`PythonRDD` que puede crashear el worker de Python en Windows incluso con
`PYSPARK_PYTHON` bien configurado. Ver el comentario al inicio de
`test_transformations.py` y `docs/observations.md` para el detalle; la solución
ya aplicada es armar los DataFrames de test con `spark.range(1).select(F.lit(...))`
en vez de `spark.createDataFrame`.

## Agregar un tenant nuevo

Ver [`docs/onboarding-tenant.md`](docs/onboarding-tenant.md). En resumen: un
archivo nuevo en `config/tenants/<codigo>.yaml`, y que las entregas de ese
tenant existan en el CSV compartido de `raw/` con su código en la columna
`pais` — no hace falta tocar código.

## Qué dejé fuera y por qué

- **Infra como código real**: `docs/infra.md` incluye un módulo Terraform
  *ilustrativo* (cómo se vería aprovisionar el storage por ambiente), no un
  despliegue completo a un Workspace de Databricks real — el alcance pedido es
  el pipeline y su CI, no la infraestructura productiva completa.
- **Orquestación externa**: no hay Airflow/Databricks Workflows programando
  corridas periódicas ni reintentos con backoff — el CLI se corre a mano o vía
  un scheduler externo que no es parte de este repo.
- **Control de acceso por tenant**: el aislamiento multi-tenant es solo por
  *path* (`data/<capa>/<tenant>/<tabla>`), no por permisos de un catálogo real
  (ej. Unity Catalog) — no hay ACLs que impidan que un proceso con acceso al
  storage lea datos de otro tenant.
- **SCD Type 1**: `dim_materials` solo implementa SCD Type 2 (que es lo que
  pide la arquitectura). Si hiciera falta corregir un dato histórico sin que
  cuente como un cambio de vigencia, no hay una ruta para eso hoy.
- **Monitoreo/alertas**: los resultados de calidad quedan en la tabla
  `quality_logs`, pero no hay dashboard ni alertas automáticas — alguien tiene
  que consultarla.

## Documentación adicional

- [`docs/observations.md`](docs/observations.md) — hallazgos y decisiones técnicas.
- [`docs/onboarding-tenant.md`](docs/onboarding-tenant.md) — cómo agregar un tenant.
- [`docs/infra.md`](docs/infra.md) — módulo Terraform ilustrativo.
- [`mentoring/`](mentoring/) — ejercicio de code review para un dev junior.