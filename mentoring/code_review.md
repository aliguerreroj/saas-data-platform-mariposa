# Code review — `bad_code.py`

Revisión del código del Anexo A. Cada observación indica el problema, un
escenario concreto donde falla o duele, y cómo se resuelve en
`good_code.py`.

## 1. Usa pandas donde corresponde Spark nativo

`pd.read_csv(file_path)` carga el archivo entero en la memoria de un solo
proceso Python. Esto contradice el propósito de tener una `SparkSession`
levantada: se paga el costo de arrancar un cluster Spark y después no se
usa para nada hasta el final (`spark.createDataFrame(out)`), cuando ya se
perdió toda la ventaja de distribuir el trabajo. Con un archivo de unos
pocos GB esto ya no entra en memoria y el proceso muere con
`MemoryError`, mucho antes de llegar al tamaño de datos que Spark está
pensado para manejar. **Fix:** `good_code.py` lee el CSV directamente como
`DataFrame` de Spark (`spark.read.csv(...)`) y nunca pasa por pandas.

## 2. Iteración fila por fila (`for i, row in df.iterrows()`)

`iterrows()` es, dentro de pandas mismo, de las formas más lentas de
recorrer un DataFrame — reconstruye cada fila como una `Series` de Python en
cada iteración. Si este patrón se trasladara a un DataFrame de Spark (un
error todavía peor que a veces se ve en código migrado apurado), sería
directamente inviable: forzaría traer todo el dataset al driver con
`.collect()` antes de poder iterar, perdiendo el paralelismo por completo.
**Fix:** `good_code.py` no itera nada — la conversión de unidades y el
cálculo del total son expresiones de columna (`F.when(...).otherwise(...)`,
`cantidad * precio`) que Spark ejecuta de forma distribuida sobre todas las
filas a la vez.

## 3. Lógica de negocio hardcoded

`"ZPRE"`, `"ZVE1"`, `"CS"` y `20` están escritos directamente en el `if`.
Si mañana cambia el multiplicador de cajas, o se agrega un tercer tipo de
entrega "de rutina", hay que editar código y volver a desplegar — el mismo
problema que resolvimos en el pipeline real sacando estos valores a
`config/base.yaml` (`case_units_multiplier`, `routine_delivery_types`).
**Fix:** `good_code.py` las mueve a una dataclass `DeliveryRules`,
inyectable como parámetro — standalone acá, pero el mismo lugar donde en el
pipeline real se leería desde configuración.

## 4. Ausencia de validaciones y tipado

No hay ninguna verificación de que las columnas esperadas existan, ni de
que `cantidad`/`precio` sean valores numéricos válidos. Si al CSV le falta
la columna `unidad`, o si `cantidad` viene vacía en una fila, el código
explota con un `KeyError` o un `TypeError` genérico en medio del loop —
sin decir en qué fila, ni por qué. **Fix:** `good_code.py` valida las
columnas requeridas al principio (`_validate_schema`, con un mensaje
explícito de qué falta) y castea `cantidad`/`precio` a `decimal` de forma
explícita, en vez de operar sobre lo que pandas haya inferido.

## 5. Naming inconsistente

`df`, `out`, `sdf`, `qty`, `i`, `row`, `result` mezclan niveles de
abstracción y no dicen nada sobre lo que contienen — a mitad de la función
no queda claro si `df` es el crudo o el ya filtrado. **Fix:**
`good_code.py` usa nombres descriptivos (`raw_df`, `scoped`,
`quantity_st`, `result_df`) y sigue la misma convención que el resto del
proyecto: columnas de negocio tal como vienen del origen (`pais`,
`fecha_proceso`), columnas derivadas en inglés (`cantidad_st` es la
excepción intencional acá porque así la pide el propio Anexo A, pero en
`silver.py` real es `quantity_st`).

## 6. Manejo de errores inexistente

No hay `try/except` en ningún lado. `print("done")` es la única señal de
que el proceso terminó — no distingue "terminé y proceso 500 filas" de
"terminé porque el filtro dejó todo vacío". Si el proceso se corriera para
varios países en un loop, un solo país con datos corruptos tumbaría toda
la corrida sin dejar rastro de cuáles países sí se procesaron bien.
**Fix:** `good_code.py` envuelve el procesamiento de cada tenant en
`try/except` con logging (`logger.exception`), y en `main()` sigue con el
resto de tenants si uno falla, reportando al final cuáles fallaron y por
qué — el mismo patrón de `cli.py::main()` en el pipeline real.

## 7. Escritura no idempotente

`sdf.write.mode("overwrite")` sin ningún concepto de partición sobrescribe
TODO `/tmp/output/<country>` en cada corrida. Si el propósito fuera
procesar solo "las entregas de hoy" y agregarlas a un histórico, este
código en cambio borra el histórico completo cada vez. Es exactamente el
mismo tipo de bug que encontramos y corregimos en `recompute_partition` del
pipeline real (ver `docs/observations.md`, bug b), donde faltaba acotar el
overwrite a la partición del batch actual. **Fix:** `good_code.py` usa
`spark.sql.sources.partitionOverwriteMode = "dynamic"` junto con
`partitionBy("fecha")`, para que el overwrite solo reemplace las
particiones de fecha presentes en el DataFrame que se está escribiendo, no
la carpeta completa.

## 8. Ausencia de tests

`process()` mezcla lectura de archivo, transformación y escritura en una
sola función — no se puede probar la lógica de negocio (la conversión de
unidades, el cálculo del total) sin un CSV real en disco y sin verificar
contra archivos escritos en `/tmp`. **Fix:** `good_code.py` separa
`compute_routine_deliveries` (pura, recibe un DataFrame y devuelve un
DataFrame) de `process_tenant` (que sí hace I/O) — la primera se puede
testear con un DataFrame sintético en memoria, con el mismo patrón que ya
usamos en `tests/test_transformations.py`.

## 9. Falta de soporte multi-tenant

El único parámetro es `country`, y la única invocación
(`process("data.csv", "GT")`) está hardcoded al final del archivo, se
ejecuta apenas se importa el módulo, y solo corre para un país a la vez.
No hay forma de correr "todos los tenants conocidos" sin copiar y pegar la
línea final una vez por país. **Fix:** `good_code.py` recibe una lista de
tenants por línea de comandos (`--tenants GT SV HN`), y la ejecución queda
detrás de `if __name__ == "__main__":` — importar el módulo ya no dispara
ningún procesamiento.

---

## Cómo se lo explicaría al junior  

Le diría que casi todo lo que está mal acá viene de una misma raíz: el
código resuelve el caso feliz de una sola corrida sobre un archivo chico,
pero no piensa en qué pasa la segunda vez que se corre, con datos más
grandes, o cuando algo sale mal. Son las mismas preguntas que nos hicimos
nosotros construyendo el pipeline real: ¿qué pasa si corro esto dos veces
seguidas — se duplica algo? ¿Qué pasa si una fila viene con un dato
corrupto — se cae todo el proceso, o queda un rastro de qué falló y
seguimos con el resto? ¿Qué pasa si mañana el negocio pide otro país, u
otro tipo de entrega — hay que tocar código, o alcanza con cambiar un
archivo de configuración? Ninguna de estas preguntas tiene que ver con
saber Spark mejor o peor — usar `pd.read_csv` en vez de `spark.read.csv`
es un error de una línea, fácil de corregir. Lo que de verdad distingue a
`good_code.py` de `bad_code.py` es haberse hecho esas preguntas antes de
escribir la función, no después de que algo se rompió en producción. Por
eso, más que memorizar "usá Spark nativo" o "no hardcodees strings", la
idea que le dejaría es: cada vez que escribas una función que lee datos,
los transforma y los guarda, preguntate primero qué pasa la segunda vez
que la corrés — esa sola pregunta ya te lleva a separar la lógica de la
I/O, a pensar en idempotencia, y a no hardcodear nada que pueda cambiar.

### Temas que le pediría investigar por su cuenta

- **Ejecución distribuida vs. modo local**: por qué `pd.read_csv` +
  `iterrows()` rompe el paralelismo aunque el resultado final se envuelva
  en un `spark.createDataFrame` — qué hace un `DataFrame` de Spark distinto
  a uno de pandas por dentro (plan lógico/físico, `explain()`).
- **Particionamiento y `partitionOverwriteMode`**: la diferencia entre
  `overwrite` a secas, `overwrite` con `partitionBy` + modo `dynamic`, y
  `replaceWhere` (lo que usa `delta_io.py` en el pipeline real) — cuándo
  usar cada uno y por qué el bug de `bad_code.py` (punto 7) es tan fácil de
  cometer sin saber esto.
- **Delta Lake vs. Parquet plano**: qué gana el proyecto real con
  `MERGE INTO` y el transaction log de Delta que `good_code.py` (Parquet
  simple) no tiene — ACID, time travel, `MERGE` para upserts sin tener que
  sobrescribir toda una partición.
- **Testing de transformaciones Spark**: cómo se construye un DataFrame
  sintético en memoria para testear sin leer un CSV real (el patrón
  `_rows_to_df` de `tests/test_transformations.py`), y por qué separar
  lógica pura (`compute_routine_deliveries`) de I/O (`process_tenant`) es
  lo que hace eso posible.
- **Manejo de errores en pipelines batch**: la diferencia entre fallar
  rápido (`fail_fast`) y seguir procesando lo que se pueda reportando al
  final qué falló (el patrón de `main()` en `good_code.py` y en
  `cli.py` real) — cuándo conviene cada estrategia.