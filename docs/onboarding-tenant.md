# Onboarding de un tenant nuevo

Agregar un tenant al pipeline es, en el caso normal, un cambio de
**configuración**, no de código: el CLI descubre los tenants conocidos
leyendo los archivos que hay en `config/tenants/`, y cada capa
(Bronze/Silver/Gold) ya aísla los datos por tenant automáticamente a
partir del código que le pasás por `--tenant`.

## Pasos

**1) Crear `config/tenants/<codigo>.yaml`.**

El código va en minúscula y es el mismo valor que aparece en la columna
`pais` del CSV de entregas (Bronze lo normaliza a minúscula igual, pero
usar minúscula desde el archivo evita confusión). Ejemplo, para un tenant
nuevo de Costa Rica (`cr`):

```yaml
# Overrides específicos del tenant cr (Costa Rica).
tenant:
  code: "cr"
  display_name: "Costa Rica"
```

Este archivo es también lo que hace que el tenant aparezca al correr
`--tenant all` — `list_known_tenants()` simplemente lista los `.yaml` que
hay en esta carpeta, no hace falta tocar `cli.py` ni ningún otro archivo de
código.

Si el tenant necesitara reglas de negocio distintas a las de `base.yaml`
(otro `case_units_multiplier`, otra lista de `valid_delivery_types`, etc.),
se pueden sobrescribir en este mismo archivo bajo `business_rules:` — el
merge de configuración es `base.yaml -> config/env/<env>.yaml ->
config/tenants/<tenant>.yaml`, así que lo que pongas acá pisa tanto la
base como el ambiente.

**2) Confirmar que las entregas del tenant existen en el CSV compartido.**

`raw/global_mobility_data_entrega_productos.csv` trae las entregas de
**todos** los tenants en un solo archivo — Bronze filtra por tenant leyendo
la columna `pais` (`scope_deliveries_to_tenant_and_window`). No hace falta
(ni existe) un CSV separado por tenant: si las filas de Costa Rica ya están
en ese archivo con `pais = CR` (o `cr`, se normaliza a minúscula), el
pipeline las va a levantar apenas exista `config/tenants/cr.yaml`.

**3) El catálogo de materiales no necesita nada especial.**

`raw/materials_catalog.csv` es un catálogo global, compartido por todos los
tenants (ver `docs/observations.md`, sección de ambigüedades) — no tiene
columna de tenant y no hay que agregar nada ahí para un tenant nuevo. Cada
tenant recibe su propia copia de `dim_materials` en Silver (aislamiento por
path), pero el contenido de origen es el mismo catálogo para todos.

**4) Validar antes de correr el pipeline completo.**

```bash
python scripts/validate_configs.py
```

Esto confirma que `config/tenants/cr.yaml` compone bien contra los 3
ambientes (`dev`, `qa`, `main`) antes de gastar tiempo levantando Spark.
Si falta una clave obligatoria o el YAML tiene un error de sintaxis, este
script lo va a decir con el nombre exacto del archivo problemático.

**5) Correr el pipeline para el tenant nuevo.**

```bash
python -m saas_pipeline.cli --tenant cr --start-date 2025-01-01 --end-date 2025-12-31
```

Revisá el resumen que imprime al final (`Bronze deliveries=... | gold=...`)
y, si querés confirmar que quedó bien integrado al resto, corré
`--tenant all` una vez y verificá que `cr` aparece en el resumen final
junto a los demás tenants.

## Qué NO hace falta tocar

- `cli.py`, `config.py`, `bronze.py`, `silver.py`, `gold.py`, `quality.py`:
  ninguno tiene tenants hardcodeados. Todos reciben el tenant como
  parámetro y resuelven paths/reglas a partir de la configuración.
- `.github/workflows/ci.yml`: `scripts/validate_configs.py` ya recorre
  automáticamente **todos** los tenants que encuentre en
  `config/tenants/`, así que un tenant nuevo queda cubierto por la CI sin
  editar el workflow.

## Cuándo SÍ hace falta tocar código

Si el tenant nuevo necesita una regla de negocio que hoy no existe como
override (por ejemplo, un enriquecimiento distinto en Silver, no solo un
valor distinto de una regla existente), eso ya no es onboarding de
configuración — es un cambio de arquitectura y hay que evaluarlo aparte.