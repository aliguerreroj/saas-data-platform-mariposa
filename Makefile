.PHONY: venv install lint test validate-configs run-example clean

PYTHON ?= python3
VENV := .venv

## Crea un virtualenv en .venv (no lo activa -- ver el mensaje que imprime).
venv:
	$(PYTHON) -m venv $(VENV)
	@echo "Entorno creado. Activalo con:"
	@echo "  source $(VENV)/bin/activate      (Linux/Mac/Git Bash)"
	@echo "  $(VENV)\\Scripts\\activate         (Windows, cmd/PowerShell)"

## Instala dependencias de runtime + dev (pytest, ruff) desde requirements.txt.
install:
	pip install -r requirements.txt

## Lint: mismo comando que corre CI (.github/workflows/ci.yml).
lint:
	ruff check src tests scripts

## Tests: mismo comando que corre CI.
test:
	pytest -v

## Valida que las 3 capas de config (base -> env -> tenant) carguen sin
## error para todas las combinaciones (env, tenant) existentes.
validate-configs:
	PYTHONPATH=src python scripts/validate_configs.py

## Corrida de ejemplo del pipeline completo para un tenant, en dev.
## Ajustá --tenant / --start-date / --end-date segun lo que quieras probar.
run-example:
	PYTHONPATH=src python -m saas_pipeline.cli --env dev --tenant gt --start-date 2024-01-01 --end-date 2024-01-31

## Limpia caches de pytest/ruff y __pycache__.
clean:
	rm -rf .pytest_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +