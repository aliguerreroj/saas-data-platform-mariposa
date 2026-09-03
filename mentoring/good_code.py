"""Refactor de bad_code.py: mismo objetivo de negocio (normalizar unidades a
ST y calcular el total de una entrega), resolviendo los problemas señalados
en code_review.md.

Comparar con el pipeline real: esta misma lógica -- filtrar por tenant,
convertir CS a ST con un multiplicador configurable, filtrar por
tipo_entrega -- es literalmente lo que hacen
`saas_pipeline.bronze.scope_deliveries_to_tenant_and_window` y
`saas_pipeline.silver.normalize_units` / `filter_valid_delivery_types` en
src/saas_pipeline/. Este archivo es standalone (no importa esos módulos) a
propósito, para que se pueda leer y comparar con bad_code.py sin tener que
saltar entre archivos -- pero el patrón usado es el mismo.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeliveryRules:
    """Reglas de negocio como configuración, no como literales embebidos en
    la lógica. En el pipeline real esto vive en config/base.yaml
    (business_rules.case_units_multiplier, business_rules.routine_delivery_types)
    -- acá queda como dataclass para que este archivo sea standalone."""

    case_units_multiplier: int = 20
    routine_delivery_types: tuple[str, ...] = ("ZPRE", "ZVE1")


REQUIRED_COLUMNS = ("pais", "fecha_proceso", "tipo_entrega", "material", "unidad", "cantidad", "precio")


def _validate_schema(df: DataFrame) -> None:
    """Falla temprano y con un mensaje claro si falta una columna esperada,
    en vez de dejar que el primer .select() más adelante explote con un
    AnalysisException críptico varios pasos después."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas en el archivo de origen: {missing}")


def compute_routine_deliveries(raw_df: DataFrame, tenant: str, rules: DeliveryRules) -> DataFrame:
    """Filtra por tenant y por tipo de entrega "de rutina", y normaliza la
    cantidad a unidades (ST). Sin pandas y sin iterrows -- todo son
    expresiones de columna que Spark ejecuta de forma distribuida, igual que
    scope_deliveries_to_tenant_and_window/normalize_units en el pipeline
    real. `raw_df` es un parámetro, no algo leído adentro de la función --
    así se puede testear con un DataFrame sintético en memoria, sin CSV.
    """
    _validate_schema(raw_df)

    cantidad = F.col("cantidad").cast("decimal(18,6)")
    precio = F.col("precio").cast("decimal(18,6)")

    scoped = raw_df.filter(F.upper(F.trim(F.col("pais"))) == tenant.upper()).filter(
        F.col("tipo_entrega").isin(list(rules.routine_delivery_types))
    )

    quantity_st = F.when(
        F.col("unidad") == "CS", cantidad * F.lit(rules.case_units_multiplier)
    ).otherwise(cantidad)

    return scoped.select(
        F.lower(F.trim(F.col("pais"))).alias("pais"),
        F.col("fecha_proceso").alias("fecha"),
        F.col("material"),
        quantity_st.alias("cantidad_st"),
        (quantity_st * precio).alias("total"),
    )


def write_routine_deliveries(df: DataFrame, output_root: str, tenant: str) -> None:
    """Escritura idempotente y aislada por tenant. `partitionOverwriteMode
    "dynamic"` hace que el overwrite solo reemplace las particiones
    presentes en `df`, no la carpeta de salida entera -- el equivalente sin
    Delta del `replaceWhere` que usa `delta_io.overwrite_partition` /
    `recompute_partition` en el pipeline real (ver el bug (b) documentado en
    docs/observations.md, que era exactamente este problema)."""
    df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    path = f"{output_root}/{tenant.lower()}"
    df.write.mode("overwrite").partitionBy("fecha").parquet(path)
    logger.info("Escritas %d filas en %s", df.count(), path)


def process_tenant(
    spark: SparkSession, source_path: str, output_root: str, tenant: str, rules: DeliveryRules
) -> DataFrame:
    """Procesa UN tenant. Separada de compute_routine_deliveries a propósito
    -- esta función hace I/O (leer el CSV, escribir el resultado) y por eso
    es más difícil de testear directamente; la lógica de negocio real vive
    en compute_routine_deliveries, que sí se puede testear con datos
    sintéticos (ver tests/test_transformations.py para el mismo patrón)."""
    raw_df = spark.read.option("header", True).csv(source_path)
    try:
        result_df = compute_routine_deliveries(raw_df, tenant, rules).cache()
        write_routine_deliveries(result_df, output_root, tenant)
        return result_df
    except Exception:
        logger.exception("Fallo procesando tenant=%s desde %s", tenant, source_path)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description="Normaliza entregas de rutina a unidades ST.")
    parser.add_argument("--source", required=True, help="Path al CSV de origen.")
    parser.add_argument("--output-root", required=True, help="Carpeta base de salida.")
    parser.add_argument(
        "--tenants", required=True, nargs="+", help="Uno o mas codigos de tenant (ej. GT SV HN)."
    )
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    rules = DeliveryRules()

    failed: list[tuple[str, str]] = []
    for tenant in args.tenants:
        try:
            process_tenant(spark, args.source, args.output_root, tenant, rules)
        except Exception as exc:  # noqa: BLE001 -- se reporta y se sigue con el resto de tenants
            failed.append((tenant, str(exc)))

    spark.stop()

    if failed:
        for tenant, error in failed:
            logger.error("tenant=%s FALLO: %s", tenant, error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())