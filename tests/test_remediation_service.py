"""REM-04..06: Remediator çekirdeği; alarm -> karar -> aksiyon -> ActionEvent."""

from ingestion.schemas import AlertEvent, MetricEvent
from remediation.decide import Guards
from remediation.service import Remediator

T = "2026-11-23T10:00:00.000Z"
MB = 1 << 20


def _alert(i, source="demo-client"):
    return AlertEvent(
        f"a{i}", source, f"r{i}", "deeplog", 1.0, 0.4, T, T, "deeplog-demo-v1", "UNKNOWN"
    )


def _rem(dry_run=False, clock=None):
    calls = []
    executors = {
        "docker:restart": lambda t: calls.append(("restart", t)) or "ok",
        "ansible:disk_cleanup": lambda t: calls.append(("cleanup", t)) or "ok",
    }
    rem = Remediator(
        Guards(True, {"demo-app"}),
        {"demo-app": "demo-app", "demo-client": "demo-app"},
        executors,
        dry_run=dry_run,
        clock=clock or (lambda: 0.0),
    )
    return rem, calls


def test_early_warning_then_threshold_triggers_cleanup_once():
    rem, calls = _rem()
    rem.on_health("demo-app", {"disk_used_pct": 85.0})
    assert rem.handle(_alert(1)).status == "skipped_unknown"  # erken uyarı: henüz eşik yok
    rem.on_health("demo-app", {"disk_used_pct": 92.0})
    action = rem.handle(_alert(2))
    assert (action.fault_type, action.action, action.status) == (
        "disk_full", "ansible:disk_cleanup", "success",
    )  # fmt: skip
    assert action.target == "demo-app" and action.alert_time == T
    assert rem.handle(_alert(3)).status == "skipped_cooldown" and calls == [("cleanup", "demo-app")]


def test_unresponsive_after_three_failed_probes_and_state_reset_after_restart():
    rem, calls = _rem()
    for _ in range(3):
        rem.on_health("demo-app", None)
    assert rem.handle(_alert(1)).action == "docker:restart"
    assert rem.states["demo-app"].health_failures == 0  # eski durum yeniden tetiklemesin
    assert calls == [("restart", "demo-app")]


def test_memory_from_metrics_and_dry_run_does_not_execute():
    rem, calls = _rem(dry_run=True)
    rem.on_metric(MetricEvent("demo-app", T, 360 * MB, 384 * MB, 0, 0, 0, 0))
    action = rem.handle(_alert(1))
    assert (action.fault_type, action.status, action.dry_run) == (
        "memory_exhaustion",
        "success",
        True,
    )
    assert calls == []


def test_executor_failure_is_recorded_not_raised():
    rem, _ = _rem()
    rem.executors["docker:restart"] = lambda t: (_ for _ in ()).throw(OSError("proxy yok"))
    for _ in range(3):
        rem.on_health("demo-app", None)
    action = rem.handle(_alert(1))
    assert action.status == "failed" and "proxy yok" in action.detail


def test_only_own_sources_are_handled():
    rem, _ = _rem()
    assert rem.responsible_for(_alert(1, "demo-app"))
    assert not rem.responsible_for(_alert(1, "hdfs-replay"))
