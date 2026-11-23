import gzip
import json

import pytest
from test_stream_score import VOCAB, _pkg

from ml.detector import Detector

T = "2026-11-23T10:00:00.000Z"


def _det():
    return Detector(_pkg(), lambda content: content if content in VOCAB else None, timeout_s=60)


def test_deeplog_alert_once_per_session_on_unknown_event():
    det = _det()
    # Eğitilmemiş test modelinde bilinen olayların sırası belirsiz; bilinmeyen olay her zaman sapma.
    alerts = det.process([("blk_1", "src", "garip", T), ("blk_1", "src", "E1", T)], 0, T)
    assert [(a.kind, a.session_key, a.event_id) for a in alerts] == [
        ("deeplog", "blk_1", "UNKNOWN")
    ]
    assert alerts[0].risk_score == 1.0  # bilinmeyen olay en kötü sıra
    assert det.process([("blk_1", "src", "garip", T)], 1, T) == []  # oturum başına bir kez


def test_expire_closes_idle_sessions_with_novelty_verdict():
    det = _det()
    det.process([("bilinen", "s", m, T) for m in ("E1", "E2", "E3")], 0, T)  # [0,1,1,1,0] biliniyor
    det.process([("yeni", "s", m, T) for m in ("E4", "E4")], 0, T)
    assert det.expire(59, T) == [] and len(det.open) == 2  # henüz zaman aşımı yok
    novelty = [a.session_key for a in det.expire(60, T) if a.kind == "novelty"]
    assert novelty == ["yeni"] and det.open == {}


def test_alert_ids_are_deterministic_for_replayed_batches():
    a = _det().process([("blk_9", "s", "garip", T)], 0, T)[0]
    b = _det().process([("blk_9", "s", "garip", T)], 5, T)[0]
    assert a.alert_id == b.alert_id


def test_restored_state_continues_like_uninterrupted_run():
    events = [("blk_1", "s", "E1", T), ("blk_2", "s", "E4", T), ("blk_1", "s", "E2", T)]
    rest = [("blk_1", "s", "E3", T), ("blk_2", "s", "garip", T)]
    a = _det()
    a.process(events, 0, T)
    expected = a.process(rest, 1, T) + a.expire(100, T)

    b = _det()
    b.process(events, 0, T)
    b.last_batch = 7
    c = _det()  # yeniden başlamış iş
    c.restore(b.dump(), now=50)
    assert c.last_batch == 7 and c.open.keys() == b.open.keys()
    assert c.expire(100, T) == []  # zaman aşımı sayacı yeniden başlatıldı (50 + 60 > 100)
    assert [x.to_json() for x in c.process(rest, 51, T) + c.expire(200, T)] == [
        x.to_json() for x in expected
    ]


def test_restore_rejects_state_of_another_model():
    doc = json.loads(gzip.decompress(_det().dump()))
    doc["model"] = "baska"
    with pytest.raises(ValueError):
        _det().restore(gzip.compress(json.dumps(doc).encode()), 0)
