# Çalışma Kuralları

## Git

- **Trunk-based geliştirme:** `main` her zaman çalışır durumdadır. İş dalları kısa ömürlüdür (en fazla bir hafta).
- **Dal adı:** `<tür>/<ISSUE-ID>-kisa-aciklama`. Örnek: `feat/ING-03-replayer`
- **Commit mesajı:** Conventional Commits biçiminde yazılır. Örnek: `feat(ing): replayer hız kontrolü (ING-03)`. Kullanılan türler: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`, `exp`.
- **Birleştirme:** `main`'e yalnızca PR ile ve squash merge yöntemiyle girilir. CI yeşil değilse merge yapılmaz.
- Proje tek kişilik olsa da her değişiklik PR üzerinden geçer ve PR şablonundaki kontrol listesiyle kendi kendine incelenir. Mimariyi etkileyen PR'lar danışmanla paylaşılır.

## Sürümleme

SemVer kullanılır. Her kilometre taşı kapanışında bir etiket atılır (v0.1.0 … v1.0.0). Sürüm notları GitHub Release üzerinde, birleştirilen PR'lardan üretilir.

## Kod standartları

- Ana makinedeki araçlar Python 3.11 ile çalışır. Spark işlerinde ise sürücü ve worker'daki Python minör sürümü aynı olmalıdır (INF-04).
- Lint ve biçimlendirme için ruff kullanılır. Public fonksiyonlarda tip ipuçları zorunludur.
- Yapılandırma ortam değişkenleriyle (`.env`) yapılır. Kodda sabit host, port veya parola bulunmaz.
- Loglar stdout'a JSON satırları olarak yazılır.
- Büyük dosyalar (veri setleri, modeller, checkpoint'ler) git'e girmez; bunlar `data/` klasöründe veya HDFS'te tutulur.

## Definition of Ready

Bir iş kalemine başlanabilmesi için:

- Kabul kriteri ölçülebilir olmalı,
- Bağımlı olduğu işler tamamlanmış olmalı,
- Tahmini 12 saati geçmemeli (geçiyorsa iş bölünür).

## Definition of Done

Bir iş kalemi şu koşulların hepsi sağlandığında biter:

- Kabul kriterleri sağlandı.
- Kritik mantık için birim testleri yazıldı ve CI yeşil.
- İlgili belge güncellendi; mimari değiştiyse ADR yazıldı.
- Kod `main`'de ve issue kapatıldı.
- Deney işiyse sonuçlar `experiments/` altında ve betikle yeniden üretilebilir durumda.

## Ritüeller (tek kişilik ekibe uyarlanmış)

| Ritüel | Sıklık | Süre | Çıktı |
|---|---|---|---|
| Sprint planlama | İki haftada bir, pazartesi | 30 dk | GitHub'da sprint milestone'u |
| Çalışma notu | Çalışılan her gün | 5 dk | Issue yorumu |
| Sprint review | İki haftada bir, danışmanla | 30 dk | Demo ve toplantı notları |
| Retrospektif | İki haftada bir | 15 dk | Bir iyileştirme maddesi |
| Risk gözden geçirme | Her sprint sonunda | 10 dk | Güncel risk kaydı |

## ADR ne zaman yazılır?

Şu durumlarda yeni bir ADR yazılır: bir teknoloji seçimi yapıldığında, bir veri sözleşmesi değiştiğinde veya bir KPI'yı etkileyen yöntem değişikliği olduğunda. Format için bkz. [ADR dizini](adr/README.md).
