"""Construcción de la SparkSession con soporte Delta Lake.

En Databricks Runtime 15.x el runtime ya trae Spark + Delta preconfigurados y
este builder no es necesario (se usa la sesión que provee el cluster). Este
módulo existe para poder correr el pipeline localmente.
"""

from __future__ import annotations

import sys
import os
import re
import subprocess

from pyspark.sql import SparkSession

# JDK 17+ restringe reflection sobre módulos internos que Spark/Delta 3.5.x
# necesitan. Java 8/11 no tienen esa restricción y ni siquiera reconocen estos
# flags -- por eso los aplicamos solo si detectamos Java 17 o superior.
_JAVA17_ADD_OPENS = " ".join(
    f"--add-opens=java.base/{pkg}=ALL-UNNAMED"
    for pkg in (
        "java.lang",
        "java.lang.invoke",
        "java.lang.reflect",
        "java.io",
        "java.net",
        "java.nio",
        "java.util",
        "java.util.concurrent",
        "java.util.concurrent.atomic",
        "sun.nio.ch",
        "sun.nio.cs",
        "sun.security.action",
        "sun.util.calendar",
    )
)

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

def _java_major_version() -> int:
    """Detecta la version mayor de Java instalada (8, 11, 17, 21...)."""
    try:
        result = subprocess.run(
            ["java", "-version"], capture_output=True, text=True, timeout=10
        )
        output = result.stderr or result.stdout
        match = re.search(r'version "(\d+)(?:\.(\d+))?', output)
        if match:
            first = int(match.group(1))
            # Java 8 y anteriores se reportan como "1.8.x" -> el numero real
            # de version es el segundo grupo (8), no el primero (1).
            if first == 1 and match.group(2):
                return int(match.group(2))
            return first
    except Exception:
        pass
    return 8  # si no lo pudimos detectar, asumimos que no hacen falta los flags


def get_spark(app_name: str = "saas-data-platform", shuffle_partitions: int = 8) -> SparkSession:
    """Devuelve (o crea) la SparkSession local con Delta Lake habilitado."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.enabled", "false")
    )

    if _java_major_version() >= 17:
        builder = builder.config("spark.driver.extraJavaOptions", _JAVA17_ADD_OPENS)

    from delta import configure_spark_with_delta_pip

    builder = configure_spark_with_delta_pip(builder)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def get_plain_spark(app_name: str = "saas-pipeline-tests") -> SparkSession:
    """SparkSession sin Delta -- para tests de transformaciones puras que no
    necesitan leer/escribir Delta (evita depender de la resolución del paquete
    io.delta:delta-spark vía Maven en cada corrida de tests)."""
    builder = SparkSession.builder.appName(app_name).master("local[2]").config(
        "spark.ui.enabled", "false"
    )
    if _java_major_version() >= 17:
        builder = builder.config("spark.driver.extraJavaOptions", _JAVA17_ADD_OPENS)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
