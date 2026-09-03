# Borrador de notas para docs/observations.md

Este archivo NO es el entregable final -- es un borrador de trabajo para no
perder las decisiones que fuimos tomando mientras construíamos el pipeline.
Cuando lleguemos al Día 3 (sección 9.2 del enunciado), esto se convierte en
docs/observations.md, redactado en forma de observación (mínimo 3, cada una
cubriendo uno de los 3 ángulos que pide el enunciado).

## Candidato a "decisión con la que no estoy de acuerdo" (con propuesta alternativa)

**Bronze particiona por fecha_proceso Y tenant_id, siendo redundante.**
Cada tabla Bronze ya vive en un path exclusivo por tenant
(`data/bronze/<tenant>/<table>/...`), así que particionar ADEMÁS por
`_tenant_id` dentro de esa tabla no aporta nada -- esa columna tiene
cardinalidad 1 en cualquier tabla Bronze dada. La propia arquitectura lo
reconoce para Silver (5.4: "La separación por tenant ya viene dada por el
schema", y por eso Silver fact_deliveries NO particiona por tenant), pero
para Bronze exige explícitamente particionar por tenant_id igual (5.4:
"Bronze: particionado por fecha_proceso y tenant_id"). Propuesta
alternativa: particionar Bronze solo por fecha_proceso, igual que Silver,
por consistencia. Trade-off: implementamos la arquitectura tal como está
provista (particionamos por ambas), y dejamos esta inconsistencia
documentada acá para discutir en la sustentación, en vez de cambiar la
arquitectura unilateralmente (la propia prueba penaliza eso).

## Candidatos a "ambigüedad y cómo la resolvimos"

**Bronze ingiere todo como StringType, incluso columnas numéricas/fecha.**
Motivo: si tipamos en el momento de leer el CSV (ej. cantidad como decimal,
fecha_proceso como date), Spark en modo PERMISSIVE convierte los valores que
no calzan en NULL silenciosamente -- perderíamos la distinción entre "vino
vacío" y "vino con un valor raro que no pudimos parsear" antes incluso de
poder auditarlo. Resolución: Bronze preserva el string original tal cual
llegó; el tipado y la detección de anomalías ocurren recién en Silver, donde
sí podemos decidir con criterio (cuarentena vs descarte) en vez de perder el
dato silenciosamente.

**fecha_proceso se mantiene como String incluso en Silver fact_deliveries**
(a diferencia de valid_from/valid_to en dim_materials, que sí se tipan como
date). Motivo: fecha_proceso es la columna de partición, y necesita poder
contener el valor especial "INVALID" para las filas cuya fecha no se pudo
parsear (ver el diseño del bucket INVALID en Bronze) -- un valor que no es
una fecha válida. dim_materials no tiene ese problema (sus fechas siempre
son válidas en este dataset), así que ahí sí tipamos correctamente.

**materials_catalog en Bronze no se particiona por fecha_proceso.**
La arquitectura (5.4) habla de particionar "por fecha_proceso y tenant_id",
pero esa instrucción aplica a los datos transaccionales (deliveries). El
catálogo de materiales es una tabla de referencia/dimensión sin columna de
fecha de proceso -- se trata como un snapshot completo con overwrite total
en cada corrida, no con partición temporal.

**classify_anomalies necesita el dim_materials ACUMULADO, no el archivo
crudo del día.** La regla "material no presente en el catálogo" (5.6) debe
chequear contra el estado completo y actualizado de dim_materials (después
del MERGE INTO de SCD2), no solo contra el archivo materials_catalog.csv de
la corrida actual -- un material podría existir en el catálogo por una
corrida anterior sin aparecer en el archivo de hoy (si no tuvo cambios).
Resolución: el CLI debe correr primero el merge de dim_materials, y usar
ESE resultado (no el archivo crudo) como known_materials_df.

**Convención de idioma inglés para nombres de columna (12.5): aplica a las
columnas que inventamos nosotros, no a las que ya vienen dadas por el
esquema del CSV fuente.** Las columnas originales (pais, fecha_proceso,
material, precio, cantidad, etc.) se dejan tal como las define la sección
4.1/4.2 del enunciado -- cambiarlas sería alterar la arquitectura provista
sin necesidad. Las columnas que agregamos nosotros en Silver sí van en
inglés: quantity_st, transaction_price, material_description,
material_category, material_base_price, is_material_dimension_matched.

## Candidatos a "mejora tecnológica futura" (Horizonte 2-3)
(pendiente -- completar cuando lleguemos a Gold/CI, seguramente algo sobre
Auto Loader/streaming real para Bronze, o testing de contrato de schema)
