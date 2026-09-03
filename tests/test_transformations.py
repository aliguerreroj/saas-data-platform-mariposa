"""Tests de las transformaciones puras de bronze/silver/gold/quality.

Cada test arma su propio DataFrame chiquito y sintético (no lee los CSV
reales) -- así son rápidos, deterministas, y cada uno prueba UNA regla de
negocio puntual sin depender del resto del pipeline ni de datos externos
que puedan cambiar.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, StringType, StructField, StructType

from saas_pipeline.bronze import scope_deliveries_to_tenant_and_window
from saas_pipeline.gold import build_daily_metrics_by_delivery_type
from saas_pipeline.schemas import DELIVERIES_RAW_SCHEMA, MATERIALS_RAW_SCHEMA
from saas_pipeline.silver import (
    classify_anomalies,
    dedupe_exact,
    enrich_with_scd2,
    filter_valid_delivery_types,
    normalize_units,
    scd2_merge_materials,
    split_quarantine,
)

# --------------------------------------------------------------------------- #
# Helper para armar DataFrames de test SIN spark.createDataFrame(lista, schema)
# --------------------------------------------------------------------------- #
#
# Por qué existe esto: spark.createDataFrame(lista_de_tuplas, schema) arma un
# PythonRDD internamente (sc.parallelize + conversión de tuplas de Python a
# Row de Java), y ejecutar cualquier acción sobre ese DataFrame (collect,
# count, etc.) requiere levantar un worker de Python. En Windows esto puede
# crashear con "Python worker exited unexpectedly (crashed)" / EOFException,
# incluso con PYSPARK_PYTHON bien configurado -- nos pasó exactamente esto en
# quality.py (ver docs/observations.md), armando las filas de quality_logs
# con spark.createDataFrame(lista, schema). Ahí lo arreglamos armando cada
# fila con spark.range(1).select(F.lit(...)), que genera el dato enteramente
# en la JVM vía Catalyst y nunca pasa por un worker de Python. Acá aplicamos
# el mismo patrón para que los tests no dependan de que el worker de Python
# arranque bien en la máquina donde corran (Windows incluido).


def _rows_to_df(spark, schema: StructType, rows: list[tuple]):
    """Arma un DataFrame a partir de tuplas de Python, alineadas
    posicionalmente con `schema`, sin pasar por spark.createDataFrame."""

    def _row_df(values: tuple):
        cols = [
            F.lit(value).cast(field.dataType).alias(field.name)
            for field, value in zip(schema.fields, values)
        ]
        return spark.range(1).select(*cols)

    return reduce(lambda a, b: a.unionByName(b), (_row_df(v) for v in rows))


# Esquemas locales para los dos casos donde el test original armaba el
# DataFrame con Row(...) y dejaba que Spark infiriera el schema (esa
# inferencia también pasa por el mismo PythonRDD problemático).
_DIM_MATERIALS_TEMPORAL_SCHEMA = StructType(
    [
        StructField("material", StringType(), True),
        StructField("descripcion", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("precio_base", StringType(), True),
        StructField("valid_from", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("is_current", BooleanType(), True),
    ]
)

_FACT_FOR_GOLD_SCHEMA = StructType(
    [
        StructField("_tenant_id", StringType(), True),
        StructField("fecha_proceso", StringType(), True),
        StructField("tipo_entrega", StringType(), True),
        StructField("ruta", StringType(), True),
        StructField("transporte", StringType(), True),
        StructField("quantity_st", DecimalType(18, 6), True),
        StructField("transaction_price", DecimalType(18, 6), True),
    ]
)

# --------------------------------------------------------------------------- #
# Helpers para armar filas de deliveries sin repetir las 9 columnas cada vez.
# --------------------------------------------------------------------------- #

TECH_COLUMNS = ["_ingestion_timestamp", "_source_file", "_tenant_id", "_batch_id"]


def _delivery_row(
    pais="gt",
    fecha_proceso="20250115",
    transporte="T1",
    ruta="R1",
    tipo_entrega="Z01",
    material="MAT01",
    precio="10.50",
    cantidad="5",
    unidad="ST",
):
    return (pais, fecha_proceso, transporte, ruta, tipo_entrega, material, precio, cantidad, unidad)


def _bronze_df(spark, rows):
    """Arma un DataFrame "estilo Bronze": las 9 columnas de negocio (todas
    String, como salen de Bronze) más las 4 técnicas, para poder testear
    dedupe_exact tal como la usa cli.py."""
    df = _rows_to_df(spark, DELIVERIES_RAW_SCHEMA, rows)
    for col in TECH_COLUMNS:
        df = df.withColumn(col, F.lit(None))
    return df


# --------------------------------------------------------------------------- #
# scope_deliveries_to_tenant_and_window: filtro de tenant + bucket INVALID (5.4)
# --------------------------------------------------------------------------- #


def test_scope_deliveries_filters_tenant_and_buckets_invalid_dates(spark):
    df = _rows_to_df(
        spark,
        DELIVERIES_RAW_SCHEMA,
        [
            _delivery_row(pais="GT", fecha_proceso="20250115"),  # tenant correcto, en ventana
            _delivery_row(pais="SV", fecha_proceso="20250115"),  # otro tenant -> se descarta
            _delivery_row(pais="GT", fecha_proceso="20240101"),  # tenant correcto, FUERA de ventana
            _delivery_row(pais="GT", fecha_proceso="no-es-una-fecha"),  # no parseable -> bucket INVALID
        ],
    )

    scoped = scope_deliveries_to_tenant_and_window(
        df, tenant="gt", start_date="2025-01-01", end_date="2025-12-31"
    )
    rows = {r["fecha_proceso"] for r in scoped.collect()}

    # Se queda con: la fila en ventana, MAS la no-parseable (nunca se filtra
    # por ventana) -- pero la de otro tenant y la fuera de ventana se van.
    assert rows == {"20250115", "INVALID"}
    # `pais` queda normalizado a minúscula (bug real que encontramos y arreglamos).
    assert all(r["pais"] == "gt" for r in scoped.collect())


# --------------------------------------------------------------------------- #
# normalize_units: conversión CS -> ST (5.6 / 6.2)
# --------------------------------------------------------------------------- #

def test_normalize_units_converts_cases_to_units(spark):
    """1 CS = case_multiplier ST (acá usamos 20, el mismo valor real de
    base.yaml) -- una fila en CS debe multiplicarse, una en ST debe quedar
    exactamente igual."""
    df = _rows_to_df(
        spark,
        DELIVERIES_RAW_SCHEMA,
        [
            _delivery_row(cantidad="3", unidad="CS"),  # 3 cajas -> 60 unidades
            _delivery_row(cantidad="10", unidad="ST"),  # ya viene en unidades
        ],
    )

    result = normalize_units(df, case_multiplier=20).orderBy("unidad").collect()

    cs_row = [r for r in result if r["unidad"] == "CS"][0]
    st_row = [r for r in result if r["unidad"] == "ST"][0]
    assert cs_row["quantity_st"] == Decimal("60.000000")
    assert st_row["quantity_st"] == Decimal("10.000000")
    # transaction_price es simplemente precio casteado a decimal, sin tocar.
    assert cs_row["transaction_price"] == Decimal("10.500000")


# --------------------------------------------------------------------------- #
# filter_valid_delivery_types: separar válidas de descartadas (5.6)
# --------------------------------------------------------------------------- #


def test_filter_valid_delivery_types_splits_correctly(spark):
    df = _rows_to_df(
        spark,
        DELIVERIES_RAW_SCHEMA,
        [
            _delivery_row(tipo_entrega="Z01"),  # válido
            _delivery_row(tipo_entrega="Z04"),  # válido
            _delivery_row(tipo_entrega="XX_INVALIDO"),  # no está en la lista
        ],
    )

    validas, descartadas = filter_valid_delivery_types(df, valid_types=("Z01", "Z04", "Z09"))

    assert validas.count() == 2
    assert descartadas.count() == 1
    assert descartadas.first()["tipo_entrega"] == "XX_INVALIDO"


# --------------------------------------------------------------------------- #
# classify_anomalies: las 4 reglas de cuarentena, CON prioridad (5.6)
# --------------------------------------------------------------------------- #


def test_classify_anomalies_detects_each_rule(spark):
    known_materials = _rows_to_df(
        spark,
        MATERIALS_RAW_SCHEMA,
        [("MAT01", "desc", "cat", "5.00", "20200101", "9999-12-31", "true")],
    )
    df = _rows_to_df(
        spark,
        DELIVERIES_RAW_SCHEMA,
        [
            _delivery_row(material="MAT01"),  # sana, no debe entrar en cuarentena
            _delivery_row(fecha_proceso="INVALID", material="MAT01"),  # regla 1
            _delivery_row(cantidad="-5", material="MAT01"),  # regla 2
            _delivery_row(precio=None, material="MAT01"),  # regla 3
            _delivery_row(material="MAT_DESCONOCIDO"),  # regla 4
        ],
    )

    classified = classify_anomalies(df, known_materials)
    good, quarantine = split_quarantine(classified)

    assert good.count() == 1
    reasons = {r["_quarantine_reason"] for r in quarantine.collect()}
    assert reasons == {
        "fecha_proceso nula o inválida",
        "cantidad nula, negativa o cero",
        "precio nulo",
        "material no presente en el catálogo",
    }


def test_classify_anomalies_priority_order_when_multiple_rules_apply(spark):
    """Si una fila viola VARIAS reglas a la vez, se reporta solo la de mayor
    prioridad: fecha_proceso > cantidad > precio > material (5.6)."""
    known_materials = _rows_to_df(
        spark,
        MATERIALS_RAW_SCHEMA,
        [("MAT01", "desc", "cat", "5.00", "20200101", "9999-12-31", "true")],
    )
    # Esta fila tiene fecha inválida Y cantidad negativa Y material desconocido
    # a la vez -- debe reportar SOLO la razón de fecha (máxima prioridad).
    df = _rows_to_df(
        spark,
        DELIVERIES_RAW_SCHEMA,
        [_delivery_row(fecha_proceso="INVALID", cantidad="-1", material="MAT_DESCONOCIDO")],
    )

    classified = classify_anomalies(df, known_materials)
    assert classified.first()["_quarantine_reason"] == "fecha_proceso nula o inválida"


# --------------------------------------------------------------------------- #
# dedupe_exact: duplicados exactos por columnas de negocio (5.6)
# --------------------------------------------------------------------------- #


def test_dedupe_exact_removes_only_exact_business_duplicates(spark):
    df = _bronze_df(
        spark,
        [
            _delivery_row(ruta="R1"),
            _delivery_row(ruta="R1"),  # duplicado exacto de la anterior
            _delivery_row(ruta="R2"),  # distinta ruta -> no es duplicado
        ],
    )

    deduped = dedupe_exact(df)

    assert deduped.count() == 2
    assert set(r["ruta"] for r in deduped.select("ruta").collect()) == {"R1", "R2"}


# --------------------------------------------------------------------------- #
# scd2_merge_materials: SCD Type 2 -- is_current recomputado, no confiado (5.7)
# --------------------------------------------------------------------------- #


def test_scd2_merge_materials_recomputes_is_current_from_open_ended_date(spark):
    """materials_catalog.csv es un snapshot COMPLETO en cada corrida (no un
    delta) -- así que cuando un material cambia de precio, el archivo trae
    AMBAS filas: la vieja ya cerrada (valid_to = la fecha del cambio) y la
    nueva abierta (valid_to = 9999-12-31). scd2_merge_materials NO debe
    confiar en el is_current que trae el archivo -- debe recalcularlo él
    mismo a partir de cuál fila está open-ended (5.7)."""
    batch = _rows_to_df(
        spark,
        MATERIALS_RAW_SCHEMA,
        [
            # is_current="false" acá a propósito -- el código debe recalcularlo,
            # no leerlo tal cual del archivo.
            ("MAT01", "Tornillo", "Ferretería", "1.00", "2020-01-01", "2024-12-31", "false"),
            ("MAT01", "Tornillo", "Ferretería", "1.50", "2025-01-01", "9999-12-31", "true"),
        ],
    )

    dim = scd2_merge_materials(None, batch)

    # valid_from ya sale tipado `date` (no String) -- por eso indexamos con
    # datetime.date, no con el string original.
    rows = {r["valid_from"]: r for r in dim.collect()}
    assert dim.count() == 2
    assert rows[date(2020, 1, 1)]["is_current"] is False, "la version cerrada no es la vigente"
    assert rows[date(2025, 1, 1)]["is_current"] is True, "la version open-ended es la vigente"


def test_scd2_merge_materials_dedupes_across_runs_by_material_and_valid_from(spark):
    """Segunda corrida: el archivo trae la MISMA fila vieja (ya vista en una
    corrida anterior, ahora persistida en existing_df) más una fila nueva que
    la cierra. El merge no debe duplicar la fila vieja -- dedupe por
    (material, valid_from) -- y is_current debe migrar correctamente de la
    vieja a la nueva."""
    existing = scd2_merge_materials(
        None,
        _rows_to_df(
            spark,
            MATERIALS_RAW_SCHEMA,
            [("MAT01", "Tornillo", "Ferretería", "1.00", "2020-01-01", "9999-12-31", "true")],
        ),
    )
    assert existing.first()["is_current"] is True

    # El archivo de la corrida siguiente es un snapshot completo: incluye la
    # fila vieja (ahora cerrada) MAS la nueva.
    new_snapshot = _rows_to_df(
        spark,
        MATERIALS_RAW_SCHEMA,
        [
            ("MAT01", "Tornillo", "Ferretería", "1.00", "2020-01-01", "2024-12-31", "false"),
            ("MAT01", "Tornillo", "Ferretería", "1.50", "2025-01-01", "9999-12-31", "true"),
        ],
    )
    merged = scd2_merge_materials(existing, new_snapshot)

    assert merged.count() == 2, "no debe duplicar la fila que ya estaba en existing_df"
    rows = {r["valid_from"]: r for r in merged.collect()}
    assert rows[date(2020, 1, 1)]["is_current"] is False
    assert rows[date(2025, 1, 1)]["is_current"] is True


# --------------------------------------------------------------------------- #
# enrich_with_scd2: join TEMPORAL, no por is_current (5.7)
# --------------------------------------------------------------------------- #


def test_enrich_with_scd2_uses_temporal_join_not_is_current(spark):
    """Una entrega de 2024 debe enriquecerse con la versión de 2024 del
    material (aunque ya no sea la vigente hoy), no con la versión actual --
    justamente lo que distingue un join temporal de un join por is_current."""
    dim_materials = _rows_to_df(
        spark,
        _DIM_MATERIALS_TEMPORAL_SCHEMA,
        [
            ("MAT01", "Tornillo viejo", "Ferretería", "1.00", "2020-01-01", "2024-12-31", False),
            ("MAT01", "Tornillo nuevo", "Ferretería", "1.50", "2025-01-01", "9999-12-31", True),
        ],
    )
    deliveries = _rows_to_df(
        spark,
        DELIVERIES_RAW_SCHEMA,
        [_delivery_row(fecha_proceso="20240615", material="MAT01")],  # cae en 2024
    )

    enriched = enrich_with_scd2(deliveries, dim_materials)

    assert enriched.first()["material_description"] == "Tornillo viejo"
    assert enriched.first()["is_material_dimension_matched"] is True


# --------------------------------------------------------------------------- #
# build_daily_metrics_by_delivery_type: agregación Gold (6.4)
# --------------------------------------------------------------------------- #


def test_gold_aggregates_by_tenant_date_and_delivery_type(spark):
    """2 entregas del mismo día/tipo/tenant por rutas DISTINTAS deben sumar
    en una sola fila, con active_routes contando 2 (no 1, y no repetido)."""
    fact = _rows_to_df(
        spark,
        _FACT_FOR_GOLD_SCHEMA,
        [
            ("gt", "20250115", "Z01", "R1", "T1", Decimal("10"), Decimal("2.00")),
            ("gt", "20250115", "Z01", "R2", "T1", Decimal("5"), Decimal("2.00")),
        ],
    )

    gold = build_daily_metrics_by_delivery_type(fact)
    row = gold.first()

    assert gold.count() == 1
    assert row["total_units"] == Decimal("15")
    assert row["total_revenue"] == Decimal("30.00")
    assert row["active_routes"] == 2
    assert row["active_transports"] == 1
