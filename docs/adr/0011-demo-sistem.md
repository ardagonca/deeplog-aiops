# ADR-0011: Demo hedef sistem: oturum tanımı, log kaynakları ve arıza sinyalleri

- Durum: Kabul
- Tarih: 2026-11-23
- İlgili: [ADR-0005](0005-remediator-ayri-servis.md) (Remediator), [ADR-0006](0006-veri-ve-degerlendirme.md) (İz B), [ADR-0008](0008-sablon-kaynagi.md) (demo sistemde Drain)

## Bağlam

İz B'de (K6–K9) otonom döngü canlı bir sistem üzerinde denenir. Tasarımda dört soru açık kalmıştı:

1. **Oturum ne?** HDFS_v1'de oturum blok kimliğidir. Mimari §3.2 demo sistem için "servis adı ve kayan pencere" diyordu. Zaman penceresi, normal ve anormal davranışı aynı pencerede karıştırır; DeepLog'un dizi varsayımına da uymaz.
2. **Ani arızalar log'dan nasıl görülür?** Süreç öldüğünde ya da uygulama kilitlendiğinde uygulamanın kendi logu kesilir. Logun yokluğu tek başına bir dizi anomalisi değildir.
3. **Disk dolması güvenli ve gerçek nasıl olur?** Host diskini doldurmak tehlikelidir. Kotayı uygulama içinde taklit etmek ise gerçek bir hata (ENOSPC) üretmez.
4. **Etiket sızıntısı:** Arıza enjeksiyonu uygulamanın bir ucu üzerinden yapılırsa, o isteğin logu arızayı modele "söyler".

## Karar

**Bileşenler** (`demo-target/`):

- `demo-app`: FastAPI uygulaması (sipariş okuma/yazma, rapor üretimi). Logları `logs.raw`'a `LogEvent` olarak gönderir (`source = demo-app`).
- Yük üreteci (REM-03): Bir API ağ geçidi gibi davranır. Kendi gözlemini de loglar: istek gönderildi, yanıt kodu ve süresi, zaman aşımı, bağlantı reddedildi (`source = demo-client`).
- Arıza enjektörü (REM-02).

**Oturum = istek.** İstemci her isteğe bir `X-Request-ID` verir. Hem istemci hem uygulama o isteğin loglarını bu kimlikle `session_key` olarak yazar; tespit job'u ikisini tek oturumda birleştirir. Uygulama başlığı yalnızca `[A-Za-z0-9-]{1,64}` biçimindeyse kabul eder (log enjeksiyonuna karşı), yoksa kendisi üretir.

**Arıza sinyalleri:**

| Senaryo | Tür | Enjeksiyon | Log'daki sinyal | Beklenen aksiyon |
|---|---|---|---|---|
| Disk dolması | Kademeli | `/app/var` altındaki 64 MB tmpfs'e çöp dosya yazılır | Önce "disk usage high" uyarısı (≥ %80), sonra yazma hatası (ENOSPC) | `ansible:disk_cleanup` |
| Bellek sızıntısı | Kademeli | Uygulama içinde artan bellek ayırma | Önce "memory usage high" uyarısı (≥ %80), sonra OOM ile süreç ölümü ve istemcide bağlantı hataları | `docker:restart` |
| Süreç öldürme | Ani | `docker kill` | İstemcide "connection refused" | `docker:restart` |
| Yanıt vermeme | Ani | Uygulama tüm istekleri bekletir | İstemcide zaman aşımı | `docker:restart` |

Kademeli senaryolardaki uyarılar, servis arızalanmadan önce gelen sinyaldir. H2 (öngörü süresi > 0) bu sinyallerin DeepLog tarafından arızadan önce yakalanıp yakalanmadığıyla sınanır.

**Güvenli ve gerçek disk:** Uygulamanın veri ve geçici dizinleri 64 MB'lık bir tmpfs üzerindedir. Dolduğunda gerçek ENOSPC hatası oluşur, host diski etkilenmez. tmpfs sayfaları konteynerin bellek sınırına sayıldığı için `mem_limit` 384 MB tutulur. Böylece dolu disk tek başına OOM'a yol açmaz. Swap kapalıdır (`memswap_limit = mem_limit`): açıkken sızıntı OOM yerine yavaşlamaya dönüşüyor, arıza 88 sn yerine 165 sn'de ve süreç ölmeden geliyordu (REM-02 denemesi).

**Sızıntıya karşı:**

- `/fault/*` ve `/health` uçları log üretmez. Arıza uçları yalnızca `FAULTS_ENABLED=true` iken vardır ve port yalnızca `127.0.0.1`'e bağlıdır.
- Arıza zamanları (`failure_time`) logdan değil, enjektörün ve sağlık gözlemcisinin kayıtlarından alınır.

**Yeniden başlatma politikası:** `demo-app` `restart: "no"` ile çalışır. Çöken servisi Docker değil Remediator ayağa kaldırmalıdır; aksi halde K7 ölçülemez.

**ADR-0005'e ek:** Bellek sızıntısı için kural tablosuna `memory_exhaustion` eklenir. Koşulu: alarm var ve `mem_bytes / mem_limit_bytes ≥ 0,9`; aksiyonu `docker:restart`. Kesin eşikler REM-04'te doğrulama koşularıyla belirlenir.

## Sonuçlar

- Oturumlar kısa ve anlamlıdır (bir istek). DeepLog'un dizi modeli demo sistemde de doğal biçimde uygulanır.
- Ani arızalar istemci gözlemiyle görünür hale gelir. Bu, gerçek sistemlerdeki ağ geçidi/proxy loglarının karşılığıdır.
- Tespit job'u, satırda blok kimliği yoksa olayın kendi `session_key`'ini kullanacak şekilde genişletilir. Demo izi ayrı bir model (`deeplog-demo-v1`) ve Drain şablonlarıyla, ayrı bir tespit örneğinde çalışır (REM-03).
- Sınırlılık: Senaryolar ve sinyaller tasarım gereği belirgindir. Gerçek arızaların temsili risk R-07 olarak raporlanır.

## Değerlendirilen alternatifler

- **Kayan zaman penceresi oturumu:** Bir pencerede onlarca isteğin olayları karışır; dizi sırası anlamını yitirir.
- **Yalnızca metrik/sağlık tabanlı tespit:** Ani arızaları yakalar ama projenin log tabanlı tespit iddiasını ve H2'yi sınamaz. Sağlık bilgisi Remediator'da arıza tipini belirlemek için kullanılır, tespit için değil.
- **Uygulama içi disk kotası:** Güvenlidir ama gerçek bir dosya sistemi hatası üretmez.
