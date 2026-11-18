# ADR-0007: Spark 4.2.1 ve Python 3.10

- Durum: Kabul
- Tarih: 2026-11-17
- İlgili: [ADR-0004](0004-structured-streaming.md) içindeki "Spark 3.5" sürüm bilgisinin yerini alır. Structured Streaming kararı aynen geçerlidir.
- Düzeltme (2026-11-18): İlk metinde "3.5'in destek süresi 2026'da doldu" yazıyordu. Bu bilgi yanlıştı: 3.5'in yalnızca güvenlik düzeltmesi alan uzatılmış LTS dönemi Kasım 2027'de bitiyor. Karar değişmedi, yalnızca gerekçe düzeltildi.

## Bağlam

ADR-0004 yazılırken Spark 3.5 varsayılmıştı. Projenin geliştirme süresi Temmuz 2027'ye kadar sürüyor. Kasım 2026 itibarıyla resmi Docker imajı bulunan sürümler ve destek bitiş tarihleri şöyle ([endoflife.date](https://endoflife.date/apache-spark), 2026-11-03 güncellemesi):

| Sürüm | Çıkış | Destek bitişi | Not |
|---|---|---|---|
| 3.5.9 | 2023-09 (seri) | 2027-11-30 | Uzatılmış LTS, **yalnızca güvenlik düzeltmesi** |
| 4.1.4 | 2025-12 (seri) | 2027-06-11 | Projenin bitişinden önce destek dışı kalıyor |
| 4.2.1 | 2026-07 (seri) | 2028-01-11 | Yama sürümü 2026-10-21 tarihli |
| 4.3.0 | 2026-10-27 | 2028-04-30 | Henüz yama sürümü yok |

Bu imajların hepsi Ubuntu 22.04 tabanlıdır ve Python 3.10 içerir. Python 3.10'un upstream desteği ise Ekim 2026'da sona erdi.

Spark'ın gerektirdiği bir kural daha var: Sürücü ve executor'lar aynı Python minör sürümünü kullanmalıdır. Projede batch ve hız katmanının ortak skor kodu çalıştırması planlanıyor ([ADR-0001](0001-lambda-mimarisi.md)). Bu yüzden testlerin, makinede de Spark ile aynı Python sürümünde koşması gerekir.

## Karar

- Temel imaj `apache/spark:4.2.1-scala2.13-java17-python3-ubuntu` olacak. Bu sürüm yerleşmiş bir serinin yama sürümüdür ve desteği projenin sonuna kadar (2028-01) sürüyor. Java 17, en çok test edilmiş LTS sürümdür.
- Kafka bağlayıcısı (`spark-sql-kafka-0-10` ve bağımlılıkları) imajın içine gömülür. Jar sürümleri Spark'ın POM dosyasından alınır ve indirilirken SHA-1 ile doğrulanır. Böylece çalışma anında `--packages` ile internetten jar indirilmez.
- İmajdaki Python paketleri (numpy, pandas, pyarrow, CPU sürümü torch, drain3) `infra/images/spark/requirements.txt` dosyasında sabitlenir. Bu dosya `uv pip compile` ile üretilir.
- Makinedeki geliştirme ortamında da Python 3.10 kullanılır (`.python-version`). Ruff da bu sürümü hedef alır.
- `shortcut: Python 3.10 artık güvenlik yaması almıyor; lab ortamı için kabul edildi. Spark'ın resmi imajı Ubuntu 24.04'e geçtiğinde ya da imaja ayrı bir Python kurulduğunda yükseltilir.`

## Sonuçlar

- Spark sürümü değiştiğinde Dockerfile'daki jar sürümleri de birlikte güncellenmelidir. Yöntemi [runbook](../runbooks/altyapi.md)'ta anlatılıyor.
- pre-commit 4.7 Python 3.11 istediği için 4.6 serisinde kalındı.
- Spark 4.x'teki yeni durum API'si `transformWithStateInPandas`, RT-02 sırasında `applyInPandasWithState` ile karşılaştırılır.

## Değerlendirilen alternatifler

- **Spark 3.5:** Hâlâ destekleniyor, ancak yalnızca güvenlik düzeltmesi alıyor. Ayrıca 4.x'teki yeni Python durum API'sinden yararlanılamaz. Bu yüzden seçilmedi.
- **Spark 4.1.4:** Desteği 2027-06'da, yani projenin bitişinden önce sona eriyor.
- **Spark 4.3.0:** Çok yeni ve yama sürümü henüz çıkmadı. 4.3.1 çıktığında değerlendirilir.
- **İmaja Python 3.12 kurmak:** PySpark'ın imajdaki Python yolu ile uyum ve ek bakım maliyeti getirir. Şimdilik ertelendi.
