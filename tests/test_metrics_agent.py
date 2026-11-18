from datetime import datetime, timezone

import pytest

from ingestion.metrics_agent import cpu_pct, health, to_metric
from ingestion.schemas import MetricEvent

NOW = datetime(2026, 11, 18, 12, 0, 0, tzinfo=timezone.utc)


def _cpu(total_usage, system, cpus=6):
    return {
        "cpu_usage": {"total_usage": total_usage},
        "system_cpu_usage": system,
        "online_cpus": cpus,
    }


# Socket proxy üzerinden alınan gerçek bir one-shot yanıtın ilgili kısmı (cgroup v2).
STATS = {
    "cpu_stats": _cpu(329_425_415_000, 26_343_640_000_000),
    "memory_stats": {
        "usage": 466_862_080,
        "limit": 1_073_741_824,
        "stats": {"inactive_file": 1_552_384},
    },
    "blkio_stats": {
        "io_service_bytes_recursive": [
            {"major": 8, "minor": 48, "op": "read", "value": 4096},
            {"major": 8, "minor": 48, "op": "write", "value": 17_850_368},
        ]
    },
    "networks": {"eth0": {"rx_bytes": 377_278, "tx_bytes": 31_344_713}},
}


@pytest.mark.parametrize(
    "status, expected",
    [
        ("Up 20 minutes (healthy)", True),
        ("Up 5 seconds (unhealthy)", False),
        ("Up 2 seconds (health: starting)", None),
        ("Up 3 hours", None),
    ],
)
def test_health(status, expected):
    assert health(status) is expected


def test_cpu_pct_uses_docker_formula():
    prev = _cpu(1_000_000_000, 100_000_000_000)
    cur = _cpu(1_600_000_000, 106_000_000_000)  # 0,6 sn konteyner / 6 sn sistem, 6 çekirdek
    assert cpu_pct(prev, cur) == 60.0


def test_cpu_pct_unknown_on_first_sample_and_after_restart():
    assert cpu_pct(None, _cpu(5, 10)) is None
    assert cpu_pct(_cpu(9_000, 100), _cpu(10, 200)) is None  # sayaç sıfırlandı


def test_to_metric_from_real_stats_shape():
    event = to_metric("kafka", "Up 20 minutes (healthy)", STATS, None, NOW)
    assert event == MetricEvent(
        source="kafka",
        event_time="2026-11-18T12:00:00.000Z",
        mem_bytes=466_862_080 - 1_552_384,  # sayfa önbelleği hariç
        mem_limit_bytes=1_073_741_824,
        blk_read_bytes=4096,
        blk_write_bytes=17_850_368,
        net_rx_bytes=377_278,
        net_tx_bytes=31_344_713,
        cpu_pct=None,
        healthy=True,
    )
    assert MetricEvent.from_json(event.to_json()) == event


def test_to_metric_handles_cgroup_v1_and_missing_sections():
    stats = {
        "cpu_stats": _cpu(10, 100),
        "blkio_stats": {"io_service_bytes_recursive": [{"op": "Read", "value": 7}]},
        "networks": None,
    }
    event = to_metric("hdfs-init", "Up 1 second", stats, None, NOW)
    assert (event.blk_read_bytes, event.blk_write_bytes, event.net_rx_bytes) == (7, 0, 0)
    assert (event.mem_bytes, event.mem_limit_bytes, event.healthy) == (0, 0, None)
