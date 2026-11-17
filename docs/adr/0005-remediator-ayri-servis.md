# ADR-0005: Otonom aksiyonlar ayrı bir servis (Remediator) üzerinden

- Durum: Kabul
- Tarih: 2026-11-17

## Bağlam

Otonom aksiyonlar saniyeler sürer ve sistem üzerinde yan etki bırakır. Bu aksiyonları streaming job'unun içinde çalıştırmanın iki sakıncası var: micro-batch'ler bloklanır ve job yeniden denendiğinde aynı aksiyon iki kez çalışabilir.

Ayrıca iki kısıt var: Ansible, Windows üzerinde doğrudan çalışmaz. Öneri metnindeki Şekil 2'de de arıza tipleri ile aksiyonlar yanlış eşleştirilmiş durumda.

## Karar

- Tespit job'u yalnızca `alerts` topic'ine yazar.
- Remediator ayrı bir konteynerde çalışır ve `alerts` ile `metrics.raw` topic'lerini tüketir.
- Arıza tipini aşağıdaki kurallarla belirler.
- Aksiyonu Docker SDK ile ya da konteyner içindeki Ansible (`community.docker` koleksiyonu) ile uygular.
- Aldığı her kararı `actions` topic'ine yazar.

**Arıza tipi kuralları (v1)**

| Koşul | Arıza tipi | Aksiyon |
|---|---|---|
| Alarm var ve `disk_used_pct` ≥ 90 | `disk_full` | `ansible:disk_cleanup` (yalnızca `/app/tmp` dizini ve döndürülmüş log dosyaları silinir) |
| Alarm var ve `/health` 3 kez üst üste başarısız ya da yanıt vermiyor | `service_unresponsive` | `docker:restart` |
| Diğer durumlar | `unknown` | Aksiyon alınmaz, yalnızca bildirim gönderilir |

**Korumalar**

- Yalnızca allowlist'teki hedeflere aksiyon alınır.
- Aynı `alert_id` ikinci kez gelirse aksiyon tekrarlanmaz.
- Aynı hedefe 5 dakika içinde en fazla 1 aksiyon alınır (cooldown).
- Saatte en fazla 6 aksiyon alınır.
- `AUTOREMEDIATION_ENABLED` ayarı tüm aksiyonları kapatan kill switch'tir.
- `DRY_RUN` varsayılan olarak `true`'dur; deneylerde `false` yapılır.

## Sonuçlar

- Tespit ve aksiyon birbirinden bağımsız ölçeklenir ve test edilir.
- Remediator'ın Docker soketine erişmesi gerekir; bu bir güvenlik riskidir (R-06).
- Öneri formundaki Şekil 2 düzeltilmelidir: disk doluluğu Ansible ile disk temizlemeye, yanıt vermeyen servis Docker restart'a bağlanır (GOV-05).

## Değerlendirilen alternatifler

- **Aksiyonları streaming job'u içinde çalıştırmak:** Yukarıdaki nedenlerle seçilmedi.
- **StackStorm:** Bu iş için fazla ağır. Literatür bölümünde ilgili çalışma olarak anılabilir.
