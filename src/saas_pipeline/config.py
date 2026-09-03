"""Carga y composición de configuración jerárquica con OmegaConf.

Orden de merge (cada capa pisa a la anterior):
    config/base.yaml  ->  config/env/<env>.yaml  ->  config/tenants/<tenant>.yaml  ->  overrides de CLI

Un tenant "all" no carga overrides de tenant específico; se resuelve por tenant
individualmente en el orquestador (cli.py), que llama a load_config una vez por tenant.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from omegaconf import DictConfig, OmegaConf

CONFIG_ROOT = Path(__file__).resolve().parents[2] / "config"

VALID_ENVS = {"dev", "qa", "main"}


class ConfigError(ValueError):
    """Error de configuración: parámetro faltante, inválido o fuera de rango."""


@dataclass(frozen=True)
class PipelineConfig:
    """Configuración resuelta y validada para una corrida (un tenant, un rango de fechas)."""

    env: str
    tenant: str
    start_date: date
    end_date: date
    fail_fast: bool
    fail_on_critical: bool
    case_units_multiplier: int
    valid_delivery_types: tuple[str, ...]
    routine_delivery_types: tuple[str, ...]
    bonus_delivery_types: tuple[str, ...]
    shuffle_partitions: int
    app_name: str
    raw_path: str
    bronze_path: str
    silver_path: str
    gold_path: str
    quarantine_root: str
    quality_logs_path: str

    # --- Helpers de composición de paths (sección 5.2 de la arquitectura) ---

    def bronze_table_path(self, table: str) -> str:
        return f"{self.bronze_path}/{self.tenant}/{table}"

    def silver_table_path(self, table: str) -> str:
        return f"{self.silver_path}/{self.tenant}/{table}"

    def gold_table_path(self, table: str) -> str:
        return f"{self.gold_path}/{self.tenant}/{table}"

    def quarantine_path(self, layer: str, table: str) -> str:
        return f"{self.quarantine_root}/{layer}_quarantine/{self.tenant}/{table}"


def _raw_merged(env: str, tenant: str | None) -> DictConfig:
    if env not in VALID_ENVS:
        raise ConfigError(f"env inválido: '{env}'. Debe ser uno de {sorted(VALID_ENVS)}.")

    base_path = CONFIG_ROOT / "base.yaml"
    env_path = CONFIG_ROOT / "env" / f"{env}.yaml"
    if not base_path.exists():
        raise ConfigError(f"No se encontró config/base.yaml en {base_path}")
    if not env_path.exists():
        raise ConfigError(f"No se encontró config/env/{env}.yaml en {env_path}")

    layers = [OmegaConf.load(base_path), OmegaConf.load(env_path)]

    if tenant and tenant != "all":
        tenant_path = CONFIG_ROOT / "tenants" / f"{tenant}.yaml"
        if not tenant_path.exists():
            raise ConfigError(
                f"Tenant '{tenant}' no tiene config en config/tenants/{tenant}.yaml. "
                "Ver docs/onboarding-tenant.md para agregar un tenant nuevo."
            )
        layers.append(OmegaConf.load(tenant_path))

    return OmegaConf.merge(*layers)


def list_known_tenants() -> list[str]:
    """Tenants con config disponible (usado para resolver --tenant all)."""
    tenants_dir = CONFIG_ROOT / "tenants"
    return sorted(p.stem for p in tenants_dir.glob("*.yaml"))


def _parse_date(value: str, field_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            f"{field_name} inválido: '{value}'. Formato esperado YYYY-MM-DD."
        ) from exc


def load_config(
    env: str,
    tenant: str,
    start_date: str,
    end_date: str,
    fail_fast: bool | None = None,
    fail_on_critical: bool | None = None,
) -> PipelineConfig:
    """Compone y valida la configuración para una corrida de un tenant concreto.

    `tenant` debe ser un código concreto (no "all"): el llamador (cli.py) resuelve
    "all" a la lista de tenants conocidos y llama a esta función una vez por tenant.
    """
    if tenant == "all":
        raise ConfigError("load_config requiere un tenant concreto, no 'all'.")

    merged = _raw_merged(env, tenant)

    start = _parse_date(start_date, "start_date")
    end = _parse_date(end_date, "end_date")
    if start > end:
        raise ConfigError(f"start_date ({start}) no puede ser posterior a end_date ({end}).")

    resolved_fail_fast = (
        fail_fast if fail_fast is not None else bool(merged.execution.fail_fast)
    )
    resolved_fail_on_critical = (
        fail_on_critical
        if fail_on_critical is not None
        else bool(merged.quality.fail_on_critical)
    )

    return PipelineConfig(
        env=env,
        tenant=tenant,
        start_date=start,
        end_date=end,
        fail_fast=resolved_fail_fast,
        fail_on_critical=resolved_fail_on_critical,
        case_units_multiplier=int(merged.business_rules.case_units_multiplier),
        valid_delivery_types=tuple(merged.business_rules.valid_delivery_types),
        routine_delivery_types=tuple(merged.business_rules.routine_delivery_types),
        bonus_delivery_types=tuple(merged.business_rules.bonus_delivery_types),
        shuffle_partitions=int(merged.spark.shuffle_partitions),
        app_name=str(merged.spark.app_name),
        raw_path=str(merged.paths.raw),
        bronze_path=str(merged.paths.bronze),
        silver_path=str(merged.paths.silver),
        gold_path=str(merged.paths.gold),
        quarantine_root=str(merged.paths.quarantine_root),
        quality_logs_path=str(merged.paths.quality_logs),
    )
