"""Capa Silver: fact_deliveries (limpio, enriquecido) y dim_materials (SCD Type 2)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from saas_pipeline.config import PipelineConfig

# Las 9 columnas de negocio originales del CSV de entregas. Deduplicamos SOLO
# por estas -- las columnas técnicas (_ingestion_timestamp, _batch_id, etc.)
# no cuentan, porque dos filas de negocio idénticas siguen siendo un duplicado
# aunque hayan llegado en batches distintos.
BUSINESS_COLUMNS = [
    "pais",
    "fecha_proceso",
    "transporte",
    "ruta",
    "tipo_entrega",
    "material",
    "precio",
    "cantidad",
    "unidad",
]


def dedupe_exact(bronze_df: DataFrame) -> DataFrame:
    """Elimina duplicados exactos sobre las columnas de negocio (no las técnicas)."""
    return bronze_df.dropDuplicates(BUSINESS_COLUMNS)


def filter_valid_delivery_types(
    df: DataFrame, valid_types: tuple[str, ...]
) -> tuple[DataFrame, DataFrame]:
    """Separa filas con tipo_entrega válido de las que se descartan.

    `valid_types` viene de cfg.valid_delivery_types (base.yaml), no está fijo
    acá adentro: así la lista vive en un solo lugar.
    Devuelve (validas, descartadas) -- las descartadas no se guardan en ningún
    lado, solo se cuentan para el reporte (es distinto de cuarentena).
    """
    is_valid = F.col("tipo_entrega").isin(list(valid_types))
    return df.filter(is_valid), df.filter(~is_valid)


def classify_anomalies(df: DataFrame, known_materials_df: DataFrame) -> DataFrame:
    """Agrega la columna `_quarantine_reason` (NULL si la fila está sana).

    Las 4 reglas son exactamente las de la tabla del enunciado (5.6). Si una
    fila viola más de una a la vez, se reporta solo la primera en este orden
    de prioridad: fecha_proceso -> cantidad -> precio -> material.
    """
    # fecha_proceso ya puede venir como "INVALID" desde Bronze (fechas que no
    # se pudieron parsear en origen). to_date() sobre "INVALID" también da
    # NULL, así que esta misma condición cubre ambos casos sin código extra.
    parsed_date = F.to_date(F.col("fecha_proceso"), "yyyyMMdd")

    # cantidad/precio llegan como String desde Bronze (recordá: Bronze no
    # tipa nada). Los casteamos acá, en Silver, que es donde corresponde.
    cantidad_num = F.col("cantidad").cast("decimal(18,6)")
    precio_num = F.col("precio").cast("decimal(18,6)")

    # "material conocido" = existe en el catálogo. Comparamos contra la lista
    # de códigos distintos del catálogo (sin importar la versión/vigencia).
    known = known_materials_df.select(F.col("material").alias("_known_material")).distinct()
    with_flag = df.join(
        known, df["material"] == known["_known_material"], how="left"
    ).withColumn("_material_known", F.col("_known_material").isNotNull())

    reason = (
        F.when(parsed_date.isNull(), F.lit("fecha_proceso nula o inválida"))
        .when(cantidad_num.isNull() | (cantidad_num <= 0), F.lit("cantidad nula, negativa o cero"))
        .when(precio_num.isNull(), F.lit("precio nulo"))
        .when(~F.col("_material_known"), F.lit("material no presente en el catálogo"))
        .otherwise(F.lit(None).cast("string"))
    )

    return with_flag.withColumn("_quarantine_reason", reason).drop(
        "_known_material", "_material_known"
    )


def split_quarantine(classified_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Devuelve (filas_sanas_sin_columna_reason, filas_en_cuarentena_con_reason)."""
    good = classified_df.filter(F.col("_quarantine_reason").isNull()).drop("_quarantine_reason")
    quarantine = classified_df.filter(F.col("_quarantine_reason").isNotNull())
    return good, quarantine


def normalize_units(df: DataFrame, case_multiplier: int) -> DataFrame:
    """Convierte todo a unidad ST (unidad individual). 1 CS = `case_multiplier` ST.

    quantity_st / transaction_price son columnas que inventamos nosotros (no
    vienen del CSV origen), así que van en inglés por la convención de
    naming del enunciado -- a diferencia de `cantidad`/`precio`, que sí son
    nombres de columna dados por el esquema original y se dejan como están.
    """
    cantidad_num = F.col("cantidad").cast("decimal(18,6)")
    quantity_st = F.when(F.col("unidad") == "CS", cantidad_num * F.lit(case_multiplier)).otherwise(
        cantidad_num
    )
    # transaction_price: mismo casteo que ya usamos en classify_anomalies, pero
    # acá lo dejamos como columna definitiva (ya sabemos que no es nulo -- las
    # filas con precio nulo se fueron a cuarentena antes de llegar hasta acá).
    return df.withColumn("quantity_st", quantity_st.cast("decimal(18,6)")).withColumn(
        "transaction_price", F.col("precio").cast("decimal(18,6)")
    )


def add_delivery_flags(
    df: DataFrame, routine_types: tuple[str, ...], bonus_types: tuple[str, ...]
) -> DataFrame:
    """Agrega is_routine_delivery / is_bonus_delivery según tipo_entrega (5.6)."""
    return df.withColumn(
        "is_routine_delivery", F.col("tipo_entrega").isin(list(routine_types))
    ).withColumn("is_bonus_delivery", F.col("tipo_entrega").isin(list(bonus_types)))


def enrich_with_scd2(deliveries_df: DataFrame, dim_materials_df: DataFrame) -> DataFrame:
    """Enriquece cada entrega con la versión de dim_materials VIGENTE A LA FECHA
    de la transacción (join temporal), NO con is_current=true (5.7).

    Si el material existe en el catálogo pero ninguna versión cubre esa fecha
    (caso borde: la entrega es anterior al valid_from más antiguo registrado),
    la fila queda con las columnas de dimensión en NULL en vez de perderse --
    por eso el join es "left", no inner.
    """
    left = deliveries_df.withColumn(
        "_fecha_proceso_date", F.to_date(F.col("fecha_proceso"), "yyyyMMdd")
    )
    # Renombramos las columnas del catálogo con prefijo _ para que no choquen
    # de nombre con las de deliveries_df (ambas tienen "material", etc.).
    # material_description/category/base_price son columnas nuevas que
    # inventamos nosotros -> en inglés, misma razón que en normalize_units.
    # material_base_price se castea a decimal (5.3: columnas de negocio bien
    # tipadas) -- viene como String de dim_materials_df, igual que todo lo
    # que sale de Bronze.
    dim = dim_materials_df.select(
        F.col("material").alias("_dim_material"),
        F.col("descripcion").alias("material_description"),
        F.col("categoria").alias("material_category"),
        F.col("precio_base").cast("decimal(18,6)").alias("material_base_price"),
        F.to_date(F.col("valid_from")).alias("_valid_from"),
        F.to_date(F.col("valid_to")).alias("_valid_to"),
    )

    # La condición del join tiene las 3 partes de las que hablamos: mismo
    # material, Y la fecha de la entrega cae dentro del rango de vigencia.
    joined = left.join(
        dim,
        (left["material"] == dim["_dim_material"])
        & (left["_fecha_proceso_date"] >= dim["_valid_from"])
        & (left["_fecha_proceso_date"] <= dim["_valid_to"]),
        how="left",
    )

    # is_material_dimension_matched, no material_dimension_matched -- es un
    # flag booleano, y 5.3 exige prefijo is_ en todos los flags booleanos.
    return joined.withColumn(
        "is_material_dimension_matched", F.col("_dim_material").isNotNull()
    ).drop("_dim_material", "_valid_from", "_valid_to", "_fecha_proceso_date")


def build_fact_deliveries(df: DataFrame, cfg: PipelineConfig, dim_materials_df: DataFrame) -> DataFrame:
    """Compone normalize_units -> add_delivery_flags -> enrich_with_scd2.

    Recibe `df` ya deduplicado, filtrado y sin las filas de cuarentena (esos
    tres pasos los orquesta el CLI antes de llamar acá, porque cuarentena
    necesita guardarse aparte -- no son responsabilidad de esta función).
    """
    normalized = normalize_units(df, cfg.case_units_multiplier)
    flagged = add_delivery_flags(normalized, cfg.routine_delivery_types, cfg.bonus_delivery_types)
    return enrich_with_scd2(flagged, dim_materials_df)


# --------------------------------------------------------------------------- #
# dim_materials (SCD Type 2)
# --------------------------------------------------------------------------- #


def scd2_merge_materials(existing_df: DataFrame | None, incoming_df: DataFrame) -> DataFrame:
    """Fusiona el catálogo entrante con dim_materials existente (si ya hay una
    corrida previa) sobre la clave (material, valid_from), y RECOMPUTA
    is_current en vez de confiar en el flag de la fuente (5.7): la fila con
    valid_to = 9999-12-31 es la vigente por definición, no una opinión.

    `existing_df=None` es el caso de la primera corrida (todavía no existe
    dim_materials guardada). En un MERGE INTO real esto se traduce en un
    UPDATE + INSERT por clave, seguido de un UPDATE de is_current -- acá lo
    modelamos como una función pura (sin tocar Delta) para poder testearla.
    """
    # is_current se recalcula desde cero más abajo, así que lo descartamos de
    # los dos lados ANTES de unir. Importante: si no lo hiciéramos, esto
    # rompería -- existing_df ya tiene is_current como boolean (resultado de
    # una corrida anterior de esta misma función), mientras que incoming_df
    # lo trae como string (tal cual llega de la fuente). unionByName no puede
    # unir una columna boolean con una string.
    incoming_no_flag = incoming_df.drop("is_current")

    # Clave de negocio del merge (5.7): (material, valid_from).
    key_cols = ["material", "valid_from"]

    if existing_df is None:
        base = incoming_no_flag
    else:
        existing_no_flag = existing_df.drop("is_current")
        # Si la misma clave (material, valid_from) aparece en existing_df Y
        # en incoming_df, el snapshot de HOY (incoming_df) siempre tiene que
        # ganar -- puede traer una versión más actualizada de esa fila (ej.
        # un valid_to que antes estaba abierto y el archivo de hoy ya cerró
        # porque entró una versión nueva del material). Sin este anti-join,
        # dropDuplicates() más abajo no garantiza cuál de las dos copias
        # sobrevive: podía quedarse con la de existing_df (desactualizada) y
        # dejar el material con dos filas open-ended al mismo tiempo.
        existing_not_overwritten = existing_no_flag.join(
            incoming_no_flag.select(*key_cols).distinct(), on=key_cols, how="left_anti"
        )
        base = existing_not_overwritten.unionByName(incoming_no_flag, allowMissingColumns=True)

    deduped = base.dropDuplicates(key_cols)

    # Para cada material, la fila con valid_to=9999-12-31 es la vigente.
    is_open_ended = F.col("valid_to") == F.lit("9999-12-31")

    # Caso borde (inconsistencia de origen): si por algún motivo un material
    # no tiene NINGUNA fila open-ended, usamos como fallback la de valid_from
    # más reciente -- así siempre hay exactamente una is_current=true por SKU,
    # como exige el enunciado, incluso en ese caso raro.
    w = Window.partitionBy("material").orderBy(F.col("valid_from").desc())
    ranked = deduped.withColumn("_rank", F.row_number().over(w))
    has_open_ended = F.count(F.when(is_open_ended, True)).over(Window.partitionBy("material")) > 0
    recomputed_flag = F.when(has_open_ended, is_open_ended).otherwise(F.col("_rank") == 1)

    # Tipado final (5.3: "en columnas de negocio: tipo date"). dim_materials
    # es una tabla que construimos enteramente en Silver -- a diferencia de
    # fecha_proceso en Bronze, acá no hay ningún valor tipo "INVALID" que
    # preservar, así que no hay motivo para dejar valid_from/valid_to como
    # String. precio_base también se castea a decimal por la misma razón.
    return (
        ranked.withColumn("is_current", recomputed_flag)
        .withColumn("valid_from", F.to_date(F.col("valid_from")))
        .withColumn("valid_to", F.to_date(F.col("valid_to")))
        .withColumn("precio_base", F.col("precio_base").cast("decimal(18,6)"))
        .drop("_rank")
    )
