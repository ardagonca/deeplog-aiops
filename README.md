# DeepLog AIOps Platformu

Dağıtık sistemlerden akan log ve metrikleri gerçek zamanlı analiz eden, arızaları erken tespit eden ve tanımlı senaryolarda kendini otonom olarak iyileştiren açık kaynak bir AIOps prototipi. TÜBİTAK 2209-A kapsamında Bartın Üniversitesi'nde geliştirilmektedir.

> **Durum:** M2 – Model. Veri hattı kayıpsız çalışıyor (10.000 log/sn'de kayıp 0). HDFS_v1'de model sonuçları: olay sayımı yeniliği F1 0,990, DeepLog + oturum sonu F1 0,717 (EXP-02, ADR-0009); model `hdfs-v1` HDFS'te. Sırada M3: gerçek zamanlı tespit.

## Belgeler

| Belge | İçerik |
|---|---|
| [Proje Tüzüğü](docs/00-proje-tuzugu.md) | Amaç, kapsam, paydaşlar, **başarı ölçütleri (KPI, tek doğru kaynak)** |
| [Mimari](docs/01-mimari.md) | Bileşenler, veri akışı, Kafka topic'leri, veri sözleşmeleri, HDFS düzeni, depo yapısı |
| [Yol Haritası](docs/02-yol-haritasi.md) | Kilometre taşları, sprint takvimi, TÜBİTAK çalışma takvimiyle eşleşme |
| [Backlog](docs/03-backlog.md) | Epic'ler, iş kalemleri, kabul kriterleri, tahminler |
| [Test ve Deney Stratejisi](docs/04-test-stratejisi.md) | Test seviyeleri, metrik tanımları, deney protokolleri |
| [Risk Kaydı](docs/05-risk-kaydi.md) | Riskler, olasılık/etki, erken uyarılar, B planları |
| [Çalışma Kuralları](docs/06-calisma-kurallari.md) | Git akışı, kod standartları, DoR/DoD, ritüeller |
| [Bulut Bütçe Planı](docs/07-bulut-butcesi.md) | AWS deneyleri için maliyet tahmini (9.000 TL sınırı), zamanlama ve harcama denetimi |
| [Mimari Karar Kayıtları](docs/adr/README.md) | Teknoloji ve yöntem seçimlerinin gerekçeleri |
| [TÜBİTAK Sapma Kaydı ve Rapor Malzemesi](docs/tubitak/oneri-guncellemeleri.md) | Kabul edilen öneriden sapmalar, gerekçeleri ve sonuç raporu taslak metinleri (GOV-05) |
| [Altyapı Runbook'u](docs/runbooks/altyapi.md) | Başlatma, test, deney, sıfırlama ve sorun giderme komutları |

## Hızlı başlangıç

Gereksinimler: Docker Desktop (WSL2 için en az 12 GB bellek) ve [uv](https://docs.astral.sh/uv/).

```bash
uv sync                          # geliştirme araçları (Python 3.10)
uv run pre-commit install        # commit öncesi ruff ve diğer kontroller
uv run pytest                    # birim testleri
uv run python -m ingestion.download_dataset   # HDFS_v1 veri seti (187 MB indirme, 1,8 GB açık)

cd infra
docker compose up -d --wait      # Kafka + HDFS + Spark + ingest işi; ilk açılışta Spark imajı build edilir (~5 dk)
bash smoke_test.sh               # uçtan uca doğrulama: Kafka -> Spark -> HDFS
cd ..
uv run python -m ingestion.replayer --rate 5000 --limit 300000   # HDFS_v1 -> Kafka logs.raw -> (spark-ingest) -> HDFS
```

Arayüz adresleri, Spark işi çalıştırma ve sıfırlama adımları için bkz. [altyapı runbook'u](docs/runbooks/altyapi.md).

## Veri seti

Deneylerde [Loghub](https://github.com/logpai/loghub) HDFS_v1 veri seti kullanılır (Zhu vd., *Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics*, ISSRE 2023; [doi:10.5281/zenodo.8196385](https://doi.org/10.5281/zenodo.8196385)). Veri seti CC BY 4.0 lisanslıdır ve depoya dahil edilmez; `ingestion.download_dataset` betiği indirir ve SHA-256 ile doğrular.
