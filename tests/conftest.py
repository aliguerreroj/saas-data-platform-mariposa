"""Fixtures compartidas por todos los tests.

`spark` es session-scoped (una sola SparkSession para toda la corrida de
tests, no una por test) porque levantar una JVM es lento -- si la
recreáramos en cada test, la suite completa tardaría muchísimo más.
Usamos get_plain_spark() (sin Delta) a propósito: estos tests son sobre
transformaciones puras (bronze.py, silver.py, gold.py, quality.py), no
sobre lectura/escritura de tablas Delta -- así no dependemos de que Maven
pueda resolver el paquete io.delta:delta-spark en cada corrida de tests
(ni en CI, donde puede no tener acceso a internet).
"""

from __future__ import annotations

import pytest

from saas_pipeline.spark_session import get_plain_spark


@pytest.fixture(scope="session")
def spark():
    session = get_plain_spark(app_name="saas-pipeline-tests")
    yield session
    session.stop()
