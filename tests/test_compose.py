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


def _profiles(svc: dict) -> set[str]:
    return set(svc.get("profiles", []))


def test_services_running_together_fit_ram_budget():
    # -init servisleri açılışta çalışıp çıkar. Profilsiz servisler hep açıktır; her profil
    # (jobs: Spark sürücüsü, demo: İz B) onların yanında açılır.
    services = {n: s for n, s in _services().items() if not n.endswith("-init")}
    base = [s for s in services.values() if not _profiles(s)]
    for profile in sorted(set().union(*map(_profiles, services.values()))):
        running = base + [s for s in services.values() if profile in _profiles(s)]
        total = sum(_gb(s["mem_limit"]) for s in running)
        assert total <= RAM_BUDGET_GB, f"{profile}: toplam {total:.1f} GB > {RAM_BUDGET_GB} GB"


def test_long_running_services_have_healthcheck():
    for name, svc in _services().items():
        if not name.endswith(("-init", "-job")):
            # Tek seferlik servislerin adı -init veya -job ile biter ve healthcheck gerektirmez.
            assert "healthcheck" in svc, f"{name}: healthcheck yok"


# Bilerek yeniden başlatılmayan servisler: çökeni Remediator kaldırmalı (ADR-0011, K7).
NO_RESTART = {"demo-app"}


def test_long_running_services_restart_on_crash():
    # infra/failure_test.sh T1 senaryosu bu politikaya dayanır.
    for name, svc in _services().items():
        if name in NO_RESTART:
            assert svc.get("restart") == "no", f"{name}: restart: no olmalı"
        elif not name.endswith(("-init", "-job")):
            assert svc.get("restart") == "on-failure", f"{name}: restart: on-failure yok"
