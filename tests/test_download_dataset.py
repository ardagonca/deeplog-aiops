import hashlib
import urllib.request
import zipfile

import pytest

from ingestion import download_dataset
from ingestion.download_dataset import extract, fetch


def test_main_defaults_to_all_datasets_and_rejects_unknown(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(download_dataset, "DATA_DIR", tmp_path)
    monkeypatch.setattr(download_dataset, "fetch", lambda url, dest, sha: calls.append(dest.name))
    monkeypatch.setattr(download_dataset, "extract", lambda archive, target: False)
    assert download_dataset.main([]) == 0
    assert calls == [f"{name}.zip" for name in download_dataset.DATASETS]
    with pytest.raises(SystemExit):
        download_dataset.main(["YOK"])


def _source(tmp_path, content=b"log satiri\n" * 1000):
    src = tmp_path / "kaynak.zip"
    src.write_bytes(content)
    return src.as_uri(), hashlib.sha256(content).hexdigest()


def test_fetch_downloads_and_verifies(tmp_path):
    url, sha = _source(tmp_path)
    dest = tmp_path / "raw" / "veri.zip"
    assert fetch(url, dest, sha) is True
    assert hashlib.sha256(dest.read_bytes()).hexdigest() == sha


def test_fetch_skips_when_verified_copy_exists(tmp_path, monkeypatch):
    url, sha = _source(tmp_path)
    dest = tmp_path / "veri.zip"
    fetch(url, dest, sha)

    def no_network(*args, **kwargs):
        raise AssertionError("doğrulanmış dosya varken yeniden indirmemeli")

    monkeypatch.setattr(urllib.request, "urlopen", no_network)
    assert fetch(url, dest, sha) is False


def test_fetch_rejects_wrong_checksum_and_leaves_nothing(tmp_path):
    url, _ = _source(tmp_path)
    dest = tmp_path / "veri.zip"
    with pytest.raises(ValueError, match="SHA-256"):
        fetch(url, dest, "0" * 64)
    assert not dest.exists()
    assert not dest.with_name(dest.name + ".part").exists()


def test_fetch_redownloads_corrupted_copy(tmp_path):
    url, sha = _source(tmp_path)
    dest = tmp_path / "veri.zip"
    dest.write_bytes(b"bozuk")
    assert fetch(url, dest, sha) is True
    assert hashlib.sha256(dest.read_bytes()).hexdigest() == sha


def test_extract_once(tmp_path):
    archive = tmp_path / "a.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("HDFS.log", "satir\n")
        z.writestr("preprocessed/anomaly_label.csv", "BlockId,Label\n")
    target = tmp_path / "HDFS_v1"
    assert extract(archive, target) is True
    assert (target / "preprocessed" / "anomaly_label.csv").exists()
    assert extract(archive, target) is False


def test_extract_rejects_path_traversal(tmp_path):
    archive = tmp_path / "kotu.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("../disari.txt", "x")
    with pytest.raises(ValueError, match="Güvensiz"):
        extract(archive, tmp_path / "hedef")
    assert not (tmp_path / "disari.txt").exists()
