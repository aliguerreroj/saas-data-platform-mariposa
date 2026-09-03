"""Helpers de escritura a Delta Lake, compartidos por bronze/silver/gold."""

from __future__ import annotations

from pyspark.sql import DataFrame


def table_exists(spark, path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def overwrite_partition(
    df: DataFrame,
    path: str,
    partition_cols: list[str],
    replace_where: str,
) -> None:
    """Overwrite acotado a una o más particiones -- así logramos la idempotencia
    de Bronze: una re-ejecución del mismo rango sobrescribe exactamente esas
    particiones, sin duplicar ni tocar el resto de la tabla."""
    spark = df.sparkSession
    if table_exists(spark, path):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_where)
            .option("overwriteSchema", "true")
            .partitionBy(*partition_cols)
            .save(path)
        )
    else:
        # primera escritura: no hay ninguna partición previa que acotar.
        df.write.format("delta").mode("overwrite").partitionBy(*partition_cols).save(path)

def merge_into(
    spark,
    source_df,
    target_path: str,
    merge_keys: list[str],
    partition_cols: list[str] | None = None,
) -> None:
    """MERGE INTO por clave de negocio compuesta (idempotencia Silver fact_deliveries).

    Actualiza si la clave existe, inserta si no. Si la tabla destino no existe
    todavía, la primera corrida hace un create-as-insert-all (no hay nada
    contra qué comparar la primera vez).
    """
    from delta.tables import DeltaTable

    if not table_exists(spark, target_path):
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)
        return

    target = DeltaTable.forPath(spark, target_path)
    condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
    (
        target.alias("target")
        .merge(source_df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

def recompute_partition(
    df,
    path: str,
    partition_cols: list[str],
    replace_where: str | None = None,
) -> None:
    """Recompute completo por partición (Gold: tablas derivadas, no autoritativas).

    Si no se pasa `replace_where` explícito, lo calculamos a partir de los
    valores de partición REALMENTE PRESENTES en `df` -- así el overwrite queda
    acotado a esas particiones (igual que overwrite_partition en Bronze) y no
    borra particiones de otras fechas guardadas en corridas anteriores.
    """
    spark = df.sparkSession
    if not table_exists(spark, path):
        writer = df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)
        return

    if replace_where is None:
        conditions = []
        for col in partition_cols:
            values = [row[col] for row in df.select(col).distinct().collect()]
            quoted = ", ".join(f"'{v}'" for v in values)
            conditions.append(f"{col} IN ({quoted})")
        replace_where = " AND ".join(conditions)

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .option("overwriteSchema", "true")
        .partitionBy(*partition_cols)
        .save(path)
    )

def append_quarantine(df_reasoned, path: str) -> None:
    """Cuarentena: tabla paralela append-only con _quarantine_reason ya poblada (5.6)."""
    if df_reasoned.isEmpty():
        return
    df_reasoned.write.format("delta").mode("append").option("mergeSchema", "true").save(path)


def append_quality_logs(df, path: str) -> None:
    """quality_logs es una tabla compartida cross-tenant, append-only (5.9)."""
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
