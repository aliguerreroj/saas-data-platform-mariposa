"""Capa Gold: agregaciones de negocio. daily_metrics_by_delivery_type (6.4)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_daily_metrics_by_delivery_type(fact_deliveries_df: DataFrame) -> DataFrame:
    """Agrega fact_deliveries a granularidad (tenant_id, fecha_proceso, tipo_entrega).

    Una fila por esa combinación, con 4 métricas (6.4):
    - total_units: suma de quantity_st (ya normalizada a ST en Silver).
    - total_revenue: suma de quantity_st * transaction_price (precio de la
      transacción, NO material_base_price del catálogo -- así lo pide 6.4
      explícitamente).
    - active_routes / active_transports: count DISTINCT, no count total --
      dos entregas por la misma ruta el mismo día cuentan como 1 ruta activa,
      no 2.
    """
    # Agrupamos por _tenant_id (con guion bajo), no "tenant_id" a secas --
    # mismo nombre de columna técnica que ya usamos en Bronze/Silver (5.3),
    # aunque el texto de 5.5 lo mencione sin guion bajo en la clave de merge.
    return fact_deliveries_df.groupBy("_tenant_id", "fecha_proceso", "tipo_entrega").agg(
        F.sum("quantity_st").alias("total_units"),
        F.sum(F.col("quantity_st") * F.col("transaction_price")).alias("total_revenue"),
        F.countDistinct("ruta").alias("active_routes"),
        F.countDistinct("transporte").alias("active_transports"),
    )
