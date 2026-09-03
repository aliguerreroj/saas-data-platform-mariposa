"""Valida que TODAS las combinaciones (env, tenant) de config/ carguen sin error.

No es solo "el YAML parsea" -- load_config() hace merge de las 3 capas
(base -> env -> tenant), castea tipos y exige que existan las claves que el
pipeline necesita (business_rules, spark, paths, etc.). Si alguien agrega un
tenant nuevo y se olvida una clave, o rompe la sintaxis de un YAML, este
script lo detecta ANTES de que llegue a producción -- se corre en CI en cada
push (ver .github/workflows/ci.yml).

Usamos fechas dummy (2025-01-01 / 2025-01-31) porque load_config las exige,
pero acá no importa el rango real -- solo nos interesa que la config cargue.
"""

from __future__ import annotations

import sys

from saas_pipeline.config import VALID_ENVS, ConfigError, list_known_tenants, load_config


def main() -> int:
    tenants = list_known_tenants()
    if not tenants:
        print("No se encontró ningún tenant en config/tenants/ -- revisa la ruta.")
        return 1

    errors: list[str] = []
    for env in sorted(VALID_ENVS):
        for tenant in tenants:
            try:
                load_config(env=env, tenant=tenant, start_date="2025-01-01", end_date="2025-01-31")
            except ConfigError as exc:
                errors.append(f"[{env}/{tenant}] {exc}")

    total = len(VALID_ENVS) * len(tenants)
    if errors:
        print(f"{len(errors)}/{total} combinaciones (env, tenant) fallaron:\n")
        for e in errors:
            print(f"  - {e}")
        return 1

    print(f"OK: {total}/{total} combinaciones (env, tenant) cargaron correctamente.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
