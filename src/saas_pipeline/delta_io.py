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
