# ADR-0007: Spark 4.2.1 ve Python 3.10

- Durum: Kabul
- Tarih: 2026-11-17
- İlgili: [ADR-0004](0004-structured-streaming.md) içindeki "Spark 3.5" sürüm bilgisinin yerini alır. Structured Streaming kararı aynen geçerlidir.

## Bağlam

ADR-0004 yazılırken Spark 3.5 varsayılmıştı. Ancak 3.5 serisinin uzun dönem destek süresi 2026'da doldu. Kasım 2026 itibarıyla resmi Docker imajı bulunan sürümler şunlar:

- 4.1.4
- 4.2.1 (4.2 serisi Temmuz 2026'dan beri yayında)
- 4.3.0 (12 günlük)

Bu imajların hepsi Ubuntu 22.04 tabanlıdır ve Python 3.10 içerir. Python 3.10'un upstream desteği ise Ekim 2026'da sona erdi.

Spark'ın gerektirdiği bir kural daha var: Sürücü ve executor'lar aynı Python minör sürümünü kullanmalıdır. Projede batch ve hız katmanının ortak skor kodu çalıştırması planlanıyor ([ADR-0001](0001-lambda-mimarisi.md)). Bu yüzden testlerin, makinede de Spark ile aynı Python sürümünde koşması gerekir.

## Karar

- Temel imaj `apache/spark:4.2.1-scala2.13-java17-python3-ubuntu` olacak. Seçilen sürüm, yerleşmiş bir serinin yama sürümüdür. Java 17, en çok test edilmiş LTS sürümdür.
- Kafka bağlayıcısı (`spark-sql-kafka-0-10` ve bağımlılıkları) imajın içine gömülür. Jar sürümleri Spark'ın POM dosyasından alınır ve indirilirken SHA-1 ile doğrulanır. Böylece çalışma anında `--packages` ile internetten jar indirilmez.
- İmajdaki Python paketleri (numpy, pandas, pyarrow, CPU sürümü torch, drain3) `infra/images/spark/requirements.txt` dosyasında sabitlenir. Bu dosya `uv pip compile` ile üretilir.
- Makinedeki geliştirme ortamında da Python 3.10 kullanılır (`.python-version`). Ruff da bu sürümü hedef alır.
- `shortcut: Python 3.10 artık güvenlik yaması almıyor; lab ortamı için kabul edildi. Spark'ın resmi imajı Ubuntu 24.04'e geçtiğinde ya da imaja ayrı bir Python kurulduğunda yükseltilir.`

## Sonuçlar

- Spark sürümü değiştiğinde Dockerfile'daki jar sürümleri de birlikte güncellenmelidir. Yöntemi [runbook](../runbooks/altyapi.md)'ta anlatılıyor.
- pre-commit 4.7 Python 3.11 istediği için 4.6 serisinde kalındı.
- Spark 4.x'teki yeni durum API'si `transformWithStateInPandas`, RT-02 sırasında `applyInPandasWithState` ile karşılaştırılır.

## Değerlendirilen alternatifler

- **Spark 3.5:** Destek süresi bittiği için seçilmedi.
- **Spark 4.3.0:** Çok yeni ve yama sürümü henüz çıkmadı. 4.3.1 çıktığında değerlendirilir.
- **İmaja Python 3.12 kurmak:** PySpark'ın imajdaki Python yolu ile uyum ve ek bakım maliyeti getirir. Şimdilik ertelendi.
