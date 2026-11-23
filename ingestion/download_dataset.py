"""ING-01: Loghub veri setlerini indirir, SHA-256 ile doğrular ve data/<ad>/ altına açar.

Kullanım:  uv run python -m ingestion.download_dataset [HDFS_v1] [HDFS_2k]
Yeniden çalıştırıldığında, doğrulanmış arşivi tekrar indirmez ve açılmış veriyi tekrar açmaz.

Veri: Loghub (Zhu vd., ISSRE 2023), doi:10.5281/zenodo.8196385, CC BY 4.0.
"""

import argparse
import hashlib
import sys
import urllib.request
import zipfile
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

# ad -> (adres, SHA-256, dosya adı). .zip olanlar data/raw/'a indirilip data/<ad>/'a açılır,
# diğerleri doğrudan data/<ad>/<dosya adı> olarak kaydedilir.
DATASETS = {
    # SHA-256 ilk indirmede hesaplandı; aynı dosyanın MD5'i Zenodo'nun yayımladığı değerle eşleşti.
    "HDFS_v1": (
        "https://zenodo.org/api/records/8196385/files/HDFS_v1.zip/content",
        "04f919f2185821f23f045dca611a7586429bdabc601bd7b43f30005f8e289b01",  # md5 76a24b4d…
        "HDFS_v1.zip",
    ),
    # ML-01: elle etiketlenmiş 2.000 satır (şablon doğruluğu ölçümü). Commit'e sabitlenmiş adres.
    "HDFS_2k": (
        "https://raw.githubusercontent.com/logpai/loghub/374038faa4ca122dd7ca0730372c0b78f76f8597"
        "/HDFS/HDFS_2k.log_structured.csv",
        "729df59774e3dde934044028546d2a55d5e3d4370b9d12fcebbe4c087b2bf7b4",
        "HDFS_2k.log_structured.csv",
    ),
}

CHUNK = 1 << 20


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def fetch(url: str, dest: Path, sha256: str) -> bool:
    """Arşivi indirir ve doğrular. Doğrulanmış kopya zaten varsa False döner."""
    if dest.exists() and sha256_of(dest) == sha256:
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    digest = hashlib.sha256()
    try:
        with urllib.request.urlopen(url, timeout=60) as response, part.open("wb") as out:
            while chunk := response.read(CHUNK):
                digest.update(chunk)
                out.write(chunk)
        if digest.hexdigest() != sha256:
            raise ValueError(
                f"SHA-256 uyuşmuyor: {url}\n  beklenen {sha256}\n  gelen    {digest.hexdigest()}"
            )
        part.replace(dest)
    finally:
        part.unlink(missing_ok=True)  # yarım veya bozuk indirme geride kalmasın
    return True


def extract(archive: Path, target: Path) -> bool:
    """Arşivi açar. Daha önce tamamen açıldıysa (işaret dosyası varsa) False döner."""
    marker = target / ".extracted"
    if marker.exists() and marker.read_text(encoding="utf-8") == archive.name:
        return False
    root = target.resolve()
    with zipfile.ZipFile(archive) as z:
        for name in z.namelist():  # arşiv dışına yazmaya çalışan girdileri reddet
            if not (root / name).resolve().is_relative_to(root):
                raise ValueError(f"Güvensiz arşiv girdisi: {name}")
        z.extractall(target)
    marker.write_text(archive.name, encoding="utf-8")  # yalnızca açma başarıyla bitince yazılır
    return True


def main(argv: list[str] | None = None) -> int:
    # Windows'ta yönlendirilen çıktı cp1252 olur ve Türkçe karakterler hata verir.
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    # choices yok: argparse, nargs="*" iken varsayılan listeyi choices'a karşı yanlış denetliyor.
    parser.add_argument("datasets", nargs="*", help=f"varsayılan: hepsi ({', '.join(DATASETS)})")
    args = parser.parse_args(argv)
    if unknown := set(args.datasets) - DATASETS.keys():
        parser.error(f"bilinmeyen veri seti: {', '.join(sorted(unknown))}")
    for name in args.datasets or DATASETS:
        url, sha256, filename = DATASETS[name]
        target = DATA_DIR / name
        is_zip = filename.endswith(".zip")
        dest = DATA_DIR / "raw" / filename if is_zip else target / filename
        print(f"{name}: hazırlanıyor (gerekirse indirilir)...", flush=True)
        downloaded = fetch(url, dest, sha256)
        print(f"{name}: {'indirildi' if downloaded else 'zaten vardı'}, SHA-256 doğrulandı")
        if is_zip:
            extracted = extract(dest, target)
            print(f"{name}: {'açıldı' if extracted else 'zaten açıktı'} -> {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
