# Observaciones técnicas

Este documento reúne las decisiones y hallazgos más relevantes del desarrollo
del pipeline, organizados en los 3 ángulos pedidos en el enunciado (9.2):
una decisión con la que no estoy de acuerdo (con propuesta alternativa),
ambigüedades y cómo las resolvimos, y una mejora tecnológica a futuro. Al
final agrego una cuarta sección con evidencia concreta: 3 bugs reales que
encontramos y corregimos durante el desarrollo de los tests, con el
escenario que los dispara, el fix, y cómo verificamos que quedaron
resueltos.

## 1. Decisión con la que no estoy de acuerdo

**Bronze particiona por `fecha_proceso` Y `_tenant_id`, siendo redundante.**
Cada tabla Bronze ya vive en un path exclusivo por tenant
(`data/bronze/<tenant>/<tabla>/...`), así que particionar además por
`_tenant_id` dentro de esa tabla no aporta nada: esa columna tiene
cardinalidad 1 en cualquier tabla Bronze dada (un tenant, un path). La
propia arquitectura reconoce este razonamiento para Silver — "la separación
por tenant ya viene dada por el schema/path" es exactamente por qué
`fact_deliveries` NO particiona por tenant — pero para Bronze exige
explícitamente particionar por `fecha_proceso` y `tenant_id` de todas
formas.

**Propuesta alternativa:** particionar Bronze solo por `fecha_proceso`,
igual que Silver, por consistencia y porque una partición de cardinalidad 1
solo agrega archivos de metadata extra en el log de Delta sin beneficio de
poda de datos (*data skipping*).

**Trade-off y decisión tomada:** implementamos la arquitectura tal como está
provista (particionamos por ambas columnas) en vez de cambiarla
unilateralmente — la prueba técnica evalúa seguir el diseño dado, y la
inconsistencia queda documentada acá para poder discutirla en la
sustentación.

## 2. Ambigüedades y cómo las resolvimos

**Bronze ingiere todo como `StringType`, incluso columnas numéricas y de
fecha.** Si tipáramos en el momento de leer el CSV (`cantidad` como
decimal, `fecha_proceso` como date), Spark en modo `PERMISSIVE` convierte
silenciosamente en `NULL` cualquier valor que no calce con el tipo —
perderíamos la distinción entre "vino vacío" y "vino con un valor que no
pudimos parsear" antes incluso de poder auditarlo. Resolución: Bronze
preserva el string original tal cual llegó; el tipado y la detección de
anomalías ocurren recién en Silver, donde sí podemos decidir con criterio
(cuarentena vs. descarte) en vez de perder el dato de forma silenciosa.

**`fecha_proceso` se mantiene como `String` incluso en `fact_deliveries`**,
a diferencia de `valid_from`/`valid_to` en `dim_materials`, que sí se tipan
como `date`. Motivo: `fecha_proceso` es la columna de partición, y necesita
poder contener el valor especial `"INVALID"` para las filas cuya fecha no
se pudo parsear (el bucket INVALID que arma Bronze) — un valor que
justamente no es una fecha válida. `dim_materials` no tiene ese problema
(sus fechas siempre son válidas en este dataset), así que ahí sí se tipan
correctamente desde el inicio.

**`materials_catalog` en Bronze no se particiona por `fecha_proceso`.** La
instrucción de particionar "por `fecha_proceso` y `tenant_id`" aplica a los
datos transaccionales (`deliveries`). El catálogo de materiales es una
tabla de referencia/dimensión sin columna de fecha de proceso propia — se
trata como un snapshot completo, con overwrite total en cada corrida, no
con partición temporal.

**`classify_anomalies` necesita el `dim_materials` YA fusionado, no el
archivo crudo del día.** La regla "material no presente en el catálogo"
debe chequear contra el estado completo y actualizado de `dim_materials`
(después del `MERGE INTO` de SCD2), no solo contra el
`materials_catalog.csv` de la corrida actual — un material puede existir en
el catálogo por una corrida anterior sin aparecer en el archivo de hoy, si
no tuvo cambios de precio o descripción. Resolución: el CLI corre primero
el merge de `dim_materials`, y usa ese resultado (no el archivo crudo) como
`known_materials_df` al clasificar anomalías.

**Convención de nombres de columna en inglés: aplica solo a las columnas
que inventamos nosotros, no a las que ya vienen dadas por el esquema del
CSV fuente.** Las columnas originales (`pais`, `fecha_proceso`, `material`,
`precio`, `cantidad`, etc.) se dejan tal como las define el CSV fuente —
cambiarlas sería alterar la arquitectura provista sin necesidad. Las
columnas que agregamos nosotros en Silver sí van en inglés: `quantity_st`,
`transaction_price`, `material_description`, `material_category`,
`material_base_price`, `is_material_dimension_matched`.

**Cuarentena es append-only incluso entre re-corridas del mismo rango de
fechas**, a diferencia de `fact_deliveries`, que sí es idempotente vía
`MERGE INTO`. Cada corrida agrega sus filas de cuarentena identificadas por
`_batch_id`, sin deduplicar contra corridas anteriores — es un log
auditable de "qué se detectó roto en cada corrida", no un estado actual que
se deba reconciliar. Verificado corriendo el pipeline 2 veces seguidas
sobre el mismo tenant y rango: `dim_materials` y `fact_deliveries` se
mantuvieron estables (35 y 374 filas respectivamente), mientras que la
cuarentena creció de 7 a 14 filas — el comportamiento esperado para un log,
no para una tabla de estado.

**`materials_catalog.csv` no tiene columna de país/tenant — es un catálogo
global compartido**, pero la arquitectura exige aislamiento de
`dim_materials` por schema/path y tenant. Resolución: replicamos el mismo
catálogo global dentro del `dim_materials` de cada tenant (verificado: los
6 tenants dieron 35 versiones de materiales idénticas), respetando el
aislamiento pedido por la arquitectura sin inventar un concepto de
"catálogo compartido cross-tenant" que el enunciado no define. El
trade-off es que el mismo dato físico queda duplicado 6 veces (una por
tenant) en vez de una sola vez — aceptable para el volumen de este
dataset, pero es exactamente el tipo de cosa que se revisaría si el
catálogo creciera mucho.

## 3. Mejora tecnológica futura (horizonte 2-3)

**Ingesta incremental en Bronze vía Auto Loader / streaming, en vez de
reprocesar el CSV completo en cada corrida.** Hoy, `ingest_deliveries_bronze`
lee `raw/global_mobility_data_entrega_productos.csv` entero en cada
invocación del CLI, y lo filtra en memoria por tenant y ventana de fechas
— para `--tenant all`, eso significa releer el mismo archivo completo una
vez por cada uno de los 6 tenants. Funciona bien al volumen actual (unos
pocos cientos de filas), pero no escala: con datos reales llegando todos
los días, cada corrida terminaría re-escaneando meses o años de historia
solo para encontrar las filas nuevas del día. La alternativa a mediano
plazo es Databricks Auto Loader (`cloudFiles`) o Structured Streaming con
checkpointing: cada archivo nuevo que aterriza en el storage de origen se
procesa una sola vez, de forma incremental, sin tener que volver a leer lo
que ya se ingirió — y de paso resuelve el problema de tener que pasar
`--tenant all` y repetir el filtrado 6 veces sobre el mismo archivo.

Como mejora relacionada y más chica: hoy no hay ningún chequeo de
*contrato de schema* sobre los CSV de entrada — si el proveedor de datos
agrega, quita o renombra una columna, Bronze simplemente falla (columna
faltante) o la ignora silenciosamente (columna nueva no declarada en
`DELIVERIES_RAW_SCHEMA`/`MATERIALS_RAW_SCHEMA`). Un chequeo explícito de
compatibilidad de schema antes de leer el CSV (comparando contra el schema
declarado, con una regla de qué cambios son aceptables) convertiría esa
falla silenciosa o tardía en un error temprano y explícito.

## 4. Bugs reales encontrados durante el desarrollo (evidencia adicional)

Estos tres no son ambigüedades de diseño sino errores concretos que
encontramos escribiendo tests y verificando corridas reales — los dejo acá
como evidencia de que el pipeline fue probado de verdad, no solo escrito.

**a) `scd2_merge_materials` podía dejar dos filas `is_current=true` para el
mismo material.** Cuando la misma clave `(material, valid_from)` existía a
la vez en `existing_df` (lo ya guardado) y en `incoming_df` (el snapshot de
hoy) con valores distintos — el caso típico es un material cuyo `valid_to`
pasó de abierto (`9999-12-31`) a cerrado porque entró una versión nueva del
precio — `dropDuplicates()` no garantiza cuál de las dos copias sobrevive.
Con un script de depuración confirmamos que en ese escenario se quedaba con
la copia vieja de `existing_df`, dejando el material con dos versiones
`is_current=true` al mismo tiempo. **Fix:** un left-anti join que asegura
que el snapshot de hoy (`incoming_df`) siempre gane sobre lo ya guardado en
una colisión de clave, antes del `dropDuplicates`. **Verificación:** 2 de
los 10 tests de `tests/test_transformations.py` prueban directamente este
escenario, más un script de regresión que corrió `materials_catalog.csv`
real dos veces seguidas por `scd2_merge_materials` — el conteo se mantiene
en 35 y el invariante "exactamente un `is_current=true` por material" se
sostiene.

**b) `recompute_partition` (usado para Gold) podía sobrescribir la tabla
entera en vez de solo la partición del batch actual.** La función aceptaba
un `replace_where` opcional que el CLI nunca pasaba explícitamente,
default `None` — eso hacía que el `overwrite` corriera SIN acotar a
ninguna partición, reemplazando toda la tabla Gold con los datos del batch
actual únicamente. **Fix:** cuando no se pasa `replace_where` explícito, se
calcula automáticamente a partir de los valores distintos de partición
presentes en el DataFrame que se está escribiendo. **Verificación:** una
corrida real en dos ventanas (enero, después febrero) sobre el mismo
tenant — la tabla Gold se mantuvo en 110 filas cubriendo fechas de enero a
junio después de ambas corridas, confirmando que las particiones fuera de
la ventana de cada corrida no se tocaron.

**c) `spark.createDataFrame(lista_de_python, schema)` crashea el worker de
Python en Windows.** Tanto en `quality.py` (armando las filas de
`quality_logs`) como en los 10 tests de `test_transformations.py`, la forma
"obvia" de construir un DataFrame chico a partir de datos de Python resultó
frágil específicamente en Windows: esa función reparte los datos vía
`sc.parallelize()` y los serializa a través de un proceso "trabajador" de
Python separado, lanzado desde la JVM — mecanismo que en Windows falla con
el síntoma *"Python worker exited unexpectedly (crashed)"* incluso con
`PYSPARK_PYTHON` correctamente configurado. **Fix:** construir cada fila con
`spark.range(1).select(F.lit(...))` en vez de `spark.createDataFrame` sobre
listas de Python — esa operación corre enteramente dentro de la JVM vía
Catalyst, sin necesitar ningún proceso Python adicional. **Verificación:**
las 10 pruebas y los 5 checks de calidad sobre los 6 tenants reales pasan
sin el crash, tanto en Linux como en Windows (confirmado en la máquina
donde corre el pipeline).