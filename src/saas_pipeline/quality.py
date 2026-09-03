"""Capa Quality: validaciones sobre Silver, resultados persistidos en quality_logs (5.9).

5 checks (el enunciado pide mínimo 3), cada uno con su severidad declarada:
- 4 critical: si fallan, hay un bug real (datos corruptos que se colaron pese
  a los filtros de Silver, o el SCD2 roto).
- 1 warning: no bloquea nada, es una señal de calidad de datos para vigilar.

`run_quality_checks` arma el DataFrame para persistir en quality_logs Y una
lista plana de resultados en Python -- así cli.py puede decidir fail_on_critical
sin tener que volver a leer el DataFrame que acaba de escribir.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CRITICAL = "critical"
WARNING = "warning"
INFO = "info"

QUALITY_LOGS_SCHEMA = StructType(
    [
        StructField("_run_id", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("tenant_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("check_name", StringType(), False),
        StructField("check_severity", StringType(), False),
        StructField("records_checked", LongType(), False),
        StructField("records_failed", LongType(), False),
        StructField("check_passed", BooleanType(), False),
        StructField("executed_at", TimestampType(), False),
    ]
)


@dataclass(frozen=True)
class QualityCheckResult:
    check_name: str
    table_name: str
    check_severity: str
    records_checked: int
    records_failed: int

    @property
    def check_passed(self) -> bool:
        return self.records_failed == 0


def _check_no_invalid_or_null_dates(fact_df: DataFrame) -> QualityCheckResult:
    """CRITICAL: fact_deliveries no debe tener fecha_proceso nula o 'INVALID'.

    Red de seguridad: classify_anomalies ya debería haber mandado esas filas a
    cuarentena antes de que lleguen a fact_deliveries. Si esto falla, hay un
    bug real en la clasificación de anomalías -- no un tema de calidad de la
    fuente, sino algo que se saltó nuestro propio filtro.
    """
    total = fact_df.count()
    failed = fact_df.filter(
        F.col("fecha_proceso").isNull() | (F.col("fecha_proceso") == "INVALID")
    ).count()
    return QualityCheckResult(
        "fact_deliveries_no_invalid_dates", "fact_deliveries", CRITICAL, total, failed
    )


def _check_positive_quantities_and_prices(fact_df: DataFrame) -> QualityCheckResult:
    """CRITICAL: quantity_st > 0 y transaction_price >= 0 en fact_deliveries.

    Misma lógica que la anterior: split_quarantine ya debería haber filtrado
    cantidad nula/negativa/cero y precio nulo antes de esto.
    """
    total = fact_df.count()
    failed = fact_df.filter(
        (F.col("quantity_st") <= 0)
        | F.col("quantity_st").isNull()
        | (F.col("transaction_price") < 0)
        | F.col("transaction_price").isNull()
    ).count()
    return QualityCheckResult(
        "fact_deliveries_positive_quantity_and_price", "fact_deliveries", CRITICAL, total, failed
    )


def _check_no_duplicate_business_key(fact_df: DataFrame) -> QualityCheckResult:
    """CRITICAL: no debe haber 2 filas con la misma clave de negocio en fact_deliveries.

    build_fact_deliveries se persiste con MERGE INTO por clave compuesta -- si
    esto falla, el merge no está siendo idempotente como debería.
    """
    from saas_pipeline.silver import BUSINESS_COLUMNS

    total = fact_df.count()
    dupes = fact_df.groupBy(*BUSINESS_COLUMNS).count().filter(F.col("count") > 1)
    failed_rows = dupes.agg(F.sum("count")).first()[0] or 0
    return QualityCheckResult(
        "fact_deliveries_no_duplicate_business_key",
        "fact_deliveries",
        CRITICAL,
        total,
        int(failed_rows),
    )


def _check_material_catalog_match_rate(fact_df: DataFrame) -> QualityCheckResult:
    """WARNING: % de entregas cuyo material no matcheó ninguna vigencia del catálogo.

    No bloquea nada -- fact_deliveries guarda esas filas igual, con
    is_material_dimension_matched=false, por diseño (5.7). Pero es una señal
    de calidad de datos: si este número sube, el catálogo probablemente está
    desactualizado respecto a lo que se está entregando.
    """
    total = fact_df.count()
    failed = fact_df.filter(~F.col("is_material_dimension_matched")).count()
    return QualityCheckResult(
        "fact_deliveries_material_catalog_match_rate", "fact_deliveries", WARNING, total, failed
    )


def _check_dim_materials_single_current_per_sku(dim_df: DataFrame) -> QualityCheckResult:
    """CRITICAL: cada material debe tener EXACTAMENTE una fila is_current=true.

    Si un material tiene 0 o 2+ filas vigentes a la vez, el SCD Type 2 está
    roto -- enrich_with_scd2 podría estar enriqueciendo con la versión
    equivocada del material en algún join temporal.
    """
    total = dim_df.select("material").distinct().count()
    current_counts = dim_df.filter(F.col("is_current")).groupBy("material").count()
    wrong_count = current_counts.filter(F.col("count") != 1).count()

    materials_with_current = current_counts.select("material")
    all_materials = dim_df.select("material").distinct()
    missing_current = all_materials.join(materials_with_current, "material", "left_anti").count()

    return QualityCheckResult(
        "dim_materials_single_current_per_sku",
        "dim_materials",
        CRITICAL,
        total,
        wrong_count + missing_current,
    )


def _result_to_row_df(spark, run_id: str, batch_id: str, tenant: str, r: QualityCheckResult) -> DataFrame:
    """Arma UNA fila de quality_logs a partir de un QualityCheckResult, usando
    solo operaciones nativas de Spark (spark.range + columnas literales).

    A propósito NO usamos `spark.createDataFrame(lista_de_python, schema)`
    acá: esa función, con una lista de tuplas de Python, hace que Spark
    reparta esos datos vía `sc.parallelize` y los serialice a través de un
    proceso "trabajador" de Python (un PythonRDD) -- en Windows, ese mecanismo
    de lanzar procesos Python desde Java es frágil (falla con rutas que
    tienen espacios, antivirus, o simplemente la arquitectura que usa Spark
    para esto en Windows, que no tiene fork() como Linux/Mac). Para una tabla
    de 5 filas no vale la pena arriesgarse: `spark.range(1)` genera el dato
    enteramente en la JVM (sin tocar Python), y `.select(F.lit(...))` arma
    las columnas con expresiones de Catalyst -- cero dependencia de que
    Windows pueda lanzar un proceso Python correctamente.
    """
    return spark.range(1).select(
        F.lit(run_id).alias("_run_id"),
        F.lit(batch_id).alias("_batch_id"),
        F.lit(tenant.lower()).alias("tenant_id"),
        F.lit("silver").alias("layer"),
        F.lit(r.table_name).alias("table_name"),
        F.lit(r.check_name).alias("check_name"),
        F.lit(r.check_severity).alias("check_severity"),
        F.lit(r.records_checked).cast("long").alias("records_checked"),
        F.lit(r.records_failed).cast("long").alias("records_failed"),
        F.lit(r.check_passed).alias("check_passed"),
        F.current_timestamp().alias("executed_at"),
    )


def run_quality_checks(
    fact_deliveries_df: DataFrame,
    dim_materials_df: DataFrame,
    tenant: str,
    run_id: str,
    batch_id: str,
) -> tuple[DataFrame, list[QualityCheckResult]]:
    """Corre las 5 validaciones y arma el DataFrame listo para persistir en quality_logs."""
    spark = fact_deliveries_df.sparkSession
    results = [
        _check_no_invalid_or_null_dates(fact_deliveries_df),
        _check_positive_quantities_and_prices(fact_deliveries_df),
        _check_no_duplicate_business_key(fact_deliveries_df),
        _check_material_catalog_match_rate(fact_deliveries_df),
        _check_dim_materials_single_current_per_sku(dim_materials_df),
    ]

    row_dfs = [_result_to_row_df(spark, run_id, batch_id, tenant, r) for r in results]
    quality_logs_df = reduce(lambda a, b: a.unionByName(b), row_dfs)
    return quality_logs_df, results


def has_critical_failure(results: list[QualityCheckResult]) -> bool:
    return any(r.check_severity == CRITICAL and not r.check_passed for r in results)
