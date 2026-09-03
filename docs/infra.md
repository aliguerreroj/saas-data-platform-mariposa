# Infraestructura (ilustrativo)

Este documento y el módulo en `infra/main.tf` son **ilustrativos**: muestran
cómo se vería aprovisionar la infraestructura de storage para este pipeline
en un cloud real, no un despliegue completo ni probado. No hace falta
`terraform apply` para correr el pipeline — todo lo que se pide en esta
prueba corre local, contra el filesystem, con los paths de
`config/base.yaml`.

## Por qué esto y no un despliegue real completo

El alcance de la prueba es el pipeline y su CI, no la infraestructura
productiva completa (ver la sección "Qué dejé fuera y por qué" del
`README.md`). Lo que sí vale la pena mostrar es *cómo se traduciría* el
diseño que ya existe — capas por ambiente, aislamiento por tenant — a
infraestructura real, sin inventar un cloud provider o una arquitectura
nueva que no está pedida.

## Qué modela el módulo

`infra/main.tf` aprovisiona, para un ambiente dado (`dev`, `qa` o `main`,
el mismo valor que ya usa `--env` en el CLI):

1. **Un bucket S3 por ambiente** (`grupo-mariposa-datalake-<environment>`),
   con versionado activado. Dentro del mismo bucket, las capas
   (`data/bronze`, `data/silver`, `data/gold`, etc.) quedan como *prefijos*
   — el mismo esquema de paths que ya usa `config/base.yaml` y
   `config/env/<env>.yaml` hoy contra el filesystem local, así que no hace
   falta traducir nada al migrar de local a cloud.

2. **Una política IAM por tenant** (`known_tenants`, la misma lista de
   códigos que hoy vive en `config/tenants/*.yaml`), que solo permite
   leer/escribir bajo el prefijo de ese tenant dentro de cada capa. Esto es
   la versión real del aislamiento multi-tenant que hoy el pipeline logra
   *solo por convención de paths* — ningún mecanismo impide hoy que un
   proceso con acceso al filesystem lea o escriba en el path de otro
   tenant; en un cloud real, la política IAM sí lo impediría a nivel de
   plataforma.

## Cómo se integraría (a futuro, no implementado)

- La variable `environment` de Terraform y el flag `--env` del CLI
  apuntarían al mismo valor, para que ambiente de infraestructura y
  ambiente de configuración del pipeline nunca queden desalineados.
- `known_tenants` debería generarse a partir de `config/tenants/*.yaml` (hoy
  es una lista manual duplicada) en vez de mantenerse a mano en dos lugares
  — pendiente si este módulo pasara de ilustrativo a real.
- El pipeline pasaría de escribir en `data/<capa>/...` local a escribir en
  `s3://grupo-mariposa-datalake-<env>/data/<capa>/...`, cambiando solo la
  configuración de paths (`config/env/<env>.yaml`), sin tocar código de
  Bronze/Silver/Gold.

## Qué NO cubre este módulo

- No aprovisiona el cluster de cómputo (Databricks Workspace, cluster
  policy, etc.) ni el catálogo de datos (Unity Catalog) — solo el storage y
  el aislamiento de acceso a nivel de bucket/prefijo.
- No incluye backend remoto de Terraform (state), CI/CD para aplicar los
  cambios, ni gestión de secretos — todo eso haría falta para que este
  módulo fuera operable de verdad, y está fuera del alcance ilustrativo de
  este documento.