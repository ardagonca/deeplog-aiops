"""infra/docker-compose.yml'nin docs/01-mimari.md §7 ve §9'daki kurallara uyduğunu denetler."""

from pathlib import Path

import yaml

COMPOSE = Path(__file__).parents[1] / "infra" / "docker-compose.yml"
RAM_BUDGET_GB = 10  # WSL2 sınırı 12 GB, 2 GB işletim payı


def _services() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))["services"]


def _gb(limit: str) -> float:
    return float(limit[:-1]) * {"g": 1, "m": 1 / 1024}[limit[-1].lower()]


def test_ports_bound_to_localhost_only():
    for name, svc in _services().items():
        for port in svc.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), f"{name}: {port} localhost dışına açık"


def test_every_service_has_mem_limit():
    missing = [name for name, svc in _services().items() if "mem_limit" not in svc]
    assert not missing, f"mem_limit eksik: {missing}"


def test_services_running_together_fit_ram_budget():
    # -init servisleri açılışta çalışıp çıkar; -job (Spark sürücüsü) diğerleriyle birlikte çalışır.
    total = sum(_gb(s["mem_limit"]) for n, s in _services().items() if not n.endswith("-init"))
    assert total <= RAM_BUDGET_GB, f"Toplam {total:.1f} GB > {RAM_BUDGET_GB} GB bütçe"


def test_long_running_services_have_healthcheck():
    for name, svc in _services().items():
        if not name.endswith(("-init", "-job")):
            # Tek seferlik servislerin adı -init veya -job ile biter ve healthcheck gerektirmez.
            assert "healthcheck" in svc, f"{name}: healthcheck yok"
