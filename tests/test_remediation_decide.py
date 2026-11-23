"""REM-04/06: arıza tipi kuralları ve her koruma (test stratejisi: zorunlu kritik mantık)."""

import pytest

from ingestion.schemas import ActionEvent
from remediation.decide import Guards, TargetState, classify, decide

MB = 1 << 20


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (TargetState(disk_used_pct=95.0), ("disk_full", "ansible:disk_cleanup")),
        (TargetState(disk_used_pct=85.0), ("unknown", "none")),  # erken uyarı: henüz eşik yok
        (
            TargetState(mem_bytes=350 * MB, mem_limit_bytes=384 * MB),
            ("memory_exhaustion", "docker:restart"),
        ),
        (TargetState(mem_bytes=300 * MB, mem_limit_bytes=384 * MB), ("unknown", "none")),
        (TargetState(health_failures=3), ("service_unresponsive", "docker:restart")),
        (TargetState(health_failures=2), ("unknown", "none")),
        (TargetState(disk_used_pct=99.0, health_failures=5), ("disk_full", "ansible:disk_cleanup")),
        (TargetState(mem_bytes=1, mem_limit_bytes=None), ("unknown", "none")),
    ],
)
def test_rules(state, expected):
    assert classify(state) == expected


def _guards(**kw):
    return Guards(enabled=True, allowlist={"demo-app"}, **kw)


FULL = TargetState(disk_used_pct=99.0)


def test_duplicate_alert_is_not_acted_twice():
    g = _guards()
    assert decide(g, "a1", "demo-app", FULL, 0).status is None
    assert decide(g, "a1", "demo-app", FULL, 1000).status == "skipped_duplicate"


def test_kill_switch_and_allowlist():
    off = Guards(enabled=False, allowlist={"demo-app"})
    assert decide(off, "a1", "demo-app", FULL, 0).status == "skipped_disabled"
    assert decide(_guards(), "a1", "kafka", FULL, 0).status == "skipped_not_allowlisted"
    assert decide(_guards(), "a1", None, FULL, 0).status == "skipped_not_allowlisted"


def test_cooldown_per_target_and_unknown_does_not_consume_it():
    g = _guards(cooldown_s=300)
    assert decide(g, "u1", "demo-app", TargetState(), 0).status == "skipped_unknown"
    assert decide(g, "a1", "demo-app", FULL, 1).status is None
    assert decide(g, "a2", "demo-app", FULL, 200).status == "skipped_cooldown"
    assert decide(g, "a3", "demo-app", FULL, 302).status is None


def test_hourly_cap_slides():
    g = _guards(cooldown_s=0, max_per_hour=2)
    assert [decide(g, f"a{i}", "demo-app", FULL, t).status for i, t in enumerate((0, 10, 20))] == [
        None,
        None,
        "skipped_rate_limit",
    ]
    assert decide(g, "a9", "demo-app", FULL, 3600).status is None  # ilk aksiyon pencereden çıktı


def test_duplicate_memory_is_bounded():
    g = _guards(cooldown_s=0, max_per_hour=10**6, remember=2)
    for i in range(3):
        decide(g, f"a{i}", "demo-app", FULL, i)
    assert decide(g, "a0", "demo-app", FULL, 10).status is None  # en eski kimlik unutuldu


def test_action_event_contract_rejects_unknown_values():
    base = dict(
        action_id="x", alert_id="a", source="demo-client", target="demo-app",
        fault_type="disk_full", action="ansible:disk_cleanup", dry_run=True, status="success",
        alert_time="2026-11-23T10:00:00Z", started_at="2026-11-23T10:00:01Z",
        finished_at="2026-11-23T10:00:02Z",
    )  # fmt: skip
    assert ActionEvent.from_json(ActionEvent(**base).to_json()) == ActionEvent(**base)
    for field, bad in (("status", "ok"), ("action", "rm -rf"), ("fault_type", "x")):
        with pytest.raises(ValueError):
            ActionEvent(**{**base, field: bad})
