"""Capa Bronze: ingesta cruda a Delta, sin limpieza, con columnas técnicas."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from saas_pipeline.config import PipelineConfig
from saas_pipeline.schemas import DELIVERIES_RAW_SCHEMA, MATERIALS_RAW_SCHEMA

INVALID_DATE_BUCKET = "INVALID"


def scope_deliveries_to_tenant_and_window(
    raw_df: DataFrame, tenant: str, start_date: str, end_date: str
) -> DataFrame:
    # Paso 1: quedarnos solo con las filas de este tenant (columna `pais` del CSV).
    tenant_df = raw_df.filter(F.upper(F.trim(F.col("pais"))) == tenant.upper())

    # Paso 2: intentar convertir el texto de fecha_proceso a una fecha real.
    # Si el texto no tiene el formato yyyyMMdd (o está vacío), esto da NULL.
    parsed = F.to_date(F.col("fecha_proceso"), "yyyyMMdd")
    is_parseable = parsed.isNotNull()

    # Paso 3: ¿la fecha (si es válida) cae dentro del rango pedido?
    in_window = is_parseable & (parsed >= F.to_date(F.lit(start_date))) & (
        parsed <= F.to_date(F.lit(end_date))
    )

    # Paso 4: nos quedamos con las filas que están en ventana, MÁS todas las
    # que no tienen fecha parseable (esas nunca se filtran por ventana).
    scoped = tenant_df.filter(in_window | ~is_parseable)

    # Paso 5: la columna de partición final: la fecha real si es válida,
    # o el texto "INVALID" si no lo es. tenant_id NO se agrega acá -- se agrega
    # una sola vez, como _tenant_id, en with_technical_columns (evita duplicar
    # la misma columna con dos nombres distintos).
    # Paso 6: normalizamos `pais` a minúscula (5.3: "la columna pais del CSV
    # viene en mayúscula...el código debe normalizarla a minúscula al ingresar
    # a Bronze"). Es la columna de negocio, no _tenant_id -- las dos quedan
    # en minúscula pero son cosas distintas: `pais` es el dato de origen,
    # `_tenant_id` es la columna técnica que agrega with_technical_columns.
    return scoped.withColumn(
        "fecha_proceso",
        F.when(is_parseable, F.col("fecha_proceso")).otherwise(F.lit(INVALID_DATE_BUCKET)),
    ).withColumn("pais", F.lower(F.trim(F.col("pais"))))


def with_technical_columns(
    df: DataFrame, tenant_id: str, source_file: str, batch_id: str
) -> DataFrame:
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_tenant_id", F.lit(tenant_id))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def ingest_deliveries_bronze(
    spark: SparkSession, cfg: PipelineConfig, tenant: str, batch_id: str
) -> DataFrame:
    from saas_pipeline.delta_io import overwrite_partition

    source_file = f"{cfg.raw_path}/global_mobility_data_entrega_productos.csv"
    raw_df = spark.read.option("header", True).schema(DELIVERIES_RAW_SCHEMA).csv(source_file)

    scoped = scope_deliveries_to_tenant_and_window(
        raw_df, tenant, str(cfg.start_date), str(cfg.end_date)
    )
    bronze_df = with_technical_columns(scoped, tenant.lower(), source_file, batch_id)

    path = cfg.bronze_table_path("deliveries")
    start_str = cfg.start_date.strftime("%Y%m%d")
    end_str = cfg.end_date.strftime("%Y%m%d")
    # El INVALID siempre se re-sobrescribe entero (no tiene un rango de fechas
    # propio contra el cual acotar), el resto solo dentro de la ventana pedida.
    replace_where = (
        f"_tenant_id = '{tenant.lower()}' AND "
        f"((fecha_proceso >= '{start_str}' AND fecha_proceso <= '{end_str}') "
        f"OR fecha_proceso = '{INVALID_DATE_BUCKET}')"
    )
    overwrite_partition(
        bronze_df, path, partition_cols=["fecha_proceso", "_tenant_id"], replace_where=replace_where
    )
    return bronze_df


def ingest_materials_catalog_bronze(
    spark: SparkSession, cfg: PipelineConfig, tenant: str, batch_id: str
) -> DataFrame:
    source_file = f"{cfg.raw_path}/materials_catalog.csv"
    raw_df = spark.read.option("header", True).schema(MATERIALS_RAW_SCHEMA).csv(source_file)
    bronze_df = with_technical_columns(raw_df, tenant.lower(), source_file, batch_id)

    path = cfg.bronze_table_path("materials_catalog")
    # Catálogo maestro, sin partición de fecha: overwrite completo por tenant.
    bronze_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    return bronze_df
