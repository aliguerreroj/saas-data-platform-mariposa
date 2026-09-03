"""CLI orquestador: Bronze -> Silver -> Gold, por tenant o para todos (5.8, 6.1)."""

from __future__ import annotations

import argparse
import sys
import uuid
from dataclasses import dataclass

from saas_pipeline.bronze import ingest_deliveries_bronze, ingest_materials_catalog_bronze
from saas_pipeline.config import ConfigError, list_known_tenants, load_config
from saas_pipeline.delta_io import (
    append_quality_logs,
    append_quarantine,
    merge_into,
    recompute_partition,
    table_exists,
)
from saas_pipeline.gold import build_daily_metrics_by_delivery_type
from saas_pipeline.silver import (
    build_fact_deliveries,
    classify_anomalies,
    dedupe_exact,
    filter_valid_delivery_types,
    scd2_merge_materials,
    split_quarantine,
)
from saas_pipeline.quality import has_critical_failure, run_quality_checks
from saas_pipeline.spark_session import get_spark

SILVER_FACT_MERGE_KEYS = [
    "_tenant_id",
    "fecha_proceso",
    "transporte",
    "ruta",
    "material",
    "tipo_entrega",
]


@dataclass
class TenantRunResult:
    tenant: str
    ok: bool
    error: str | None = None
    bronze_deliveries_rows: int = 0
    fact_rows: int = 0
    quarantine_rows: int = 0
    discarded_rows: int = 0
    gold_rows: int = 0


def run_for_tenant(spark, env: str, tenant: str, start_date: str, end_date: str,
                    fail_fast: bool | None, fail_on_critical: bool | None, run_id: str) -> TenantRunResult:
    cfg = load_config(env, tenant, start_date, end_date, fail_fast, fail_on_critical)
    batch_id = f"batch_{uuid.uuid4().hex[:12]}"

    print(f"\n=== Tenant {tenant} | env={env} | batch_id={batch_id} ===")

    # --- 1) dim_materials: fusionar lo existente con el catalogo de hoy ---
    materials_bronze_df = ingest_materials_catalog_bronze(spark, cfg, tenant, batch_id)

    dim_materials_path = cfg.silver_table_path("dim_materials")
    existing_dim_materials = (
        spark.read.format("delta").load(dim_materials_path)
        if table_exists(spark, dim_materials_path)
        else None
    )
    dim_materials_df = scd2_merge_materials(existing_dim_materials, materials_bronze_df).cache()
    dim_materials_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(dim_materials_path)
    print(f"dim_materials: {dim_materials_df.count()} versiones de materiales guardadas.")

    # --- 2) deliveries: Bronze -> dedupe -> filtro tipo_entrega ---
    bronze_deliveries_df = ingest_deliveries_bronze(spark, cfg, tenant, batch_id)
    bronze_count = bronze_deliveries_df.count()

    deduped_df = dedupe_exact(bronze_deliveries_df)
    valid_df, discarded_df = filter_valid_delivery_types(deduped_df, cfg.valid_delivery_types)
    discarded_count = discarded_df.count()

    # --- 3) anomalias: usamos dim_materials YA fusionado, no el catalogo crudo ---
    classified_df = classify_anomalies(valid_df, dim_materials_df)
    good_df, quarantine_df = split_quarantine(classified_df)
    quarantine_count = quarantine_df.count()

    if quarantine_count > 0:
        quarantine_path = cfg.quarantine_path("silver", "fact_deliveries")
        append_quarantine(quarantine_df, quarantine_path)

    # --- 4) fact_deliveries: normalizar + enriquecer + MERGE INTO ---
    fact_df = build_fact_deliveries(good_df, cfg, dim_materials_df).cache()
    fact_count = fact_df.count()

    fact_path = cfg.silver_table_path("fact_deliveries")
    merge_into(spark, fact_df, fact_path, SILVER_FACT_MERGE_KEYS, partition_cols=["fecha_proceso"])

    # --- 4.5) Quality: validaciones sobre lo que Silver acaba de producir ---
    quality_logs_df, quality_results = run_quality_checks(
        fact_deliveries_df=fact_df,
        dim_materials_df=dim_materials_df,
        tenant=tenant,
        run_id=run_id,
        batch_id=batch_id,
    )
    append_quality_logs(quality_logs_df, cfg.quality_logs_path)

    if cfg.fail_on_critical and has_critical_failure(quality_results):
        failed_checks = [
            r.check_name for r in quality_results
            if r.check_severity == "critical" and not r.check_passed
        ]
        raise RuntimeError(
            f"[{tenant}] Quality check(s) crítico(s) fallaron, abortando antes de Gold: {failed_checks}"
        )
    # --- 5) Gold: agregacion a partir de lo que Silver acaba de producir ---
    gold_df = build_daily_metrics_by_delivery_type(fact_df)
    gold_count = gold_df.count()
    gold_path = cfg.gold_table_path("daily_metrics_by_delivery_type")
    recompute_partition(gold_df, gold_path, partition_cols=["fecha_proceso"])

    print(
        f"Bronze deliveries={bronze_count} | descartadas(tipo_entrega)={discarded_count} | "
        f"cuarentena={quarantine_count} | fact_deliveries={fact_count} | gold={gold_count}"
    )

    return TenantRunResult(
        tenant=tenant, ok=True,
        bronze_deliveries_rows=bronze_count, fact_rows=fact_count,
        quarantine_rows=quarantine_count, discarded_rows=discarded_count, gold_rows=gold_count,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline SAAS multi-tenant (Bronze/Silver/Gold).")
    parser.add_argument("--env", default="dev", choices=["dev", "qa", "main"])
    parser.add_argument("--tenant", required=True, help="Codigo de tenant (ej. gt) o 'all'.")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--fail-fast", dest="fail_fast", action="store_true", default=None)
    parser.add_argument("--no-fail-fast", dest="fail_fast", action="store_false")
    parser.add_argument("--fail-on-critical", dest="fail_on_critical", action="store_true", default=None)
    parser.add_argument("--no-fail-on-critical", dest="fail_on_critical", action="store_false")
    args = parser.parse_args()

    tenants = list_known_tenants() if args.tenant == "all" else [args.tenant]

    run_id = f"run_{uuid.uuid4().hex[:12]}"
    spark = get_spark()
    results: list[TenantRunResult] = []

    for tenant in tenants:
        try:
            result = run_for_tenant(
                spark, args.env, tenant, args.start_date, args.end_date,
                args.fail_fast, args.fail_on_critical, run_id,
            )
        except (ConfigError, Exception) as exc:  # noqa: BLE001 -- reportamos y seguimos/abortamos segun fail_fast
            print(f"ERROR en tenant {tenant}: {exc}")
            result = TenantRunResult(tenant=tenant, ok=False, error=str(exc))
            if args.fail_fast:
                results.append(result)
                break
        results.append(result)

    spark.stop()

    print("\n=== Resumen ===")
    failed = [r for r in results if not r.ok]
    for r in results:
        status = "OK" if r.ok else f"FALLO: {r.error}"
        print(f"  {r.tenant}: {status}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
