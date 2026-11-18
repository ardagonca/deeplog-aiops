# TÜBİTAK 2209-A Araştırma Önerisi Formu: Güncelleme Metinleri (GOV-05)

Bu belge, başvuru formunun ("2209 (2).pdf", 2025/1 dönemi taslağı) değişmesi gereken bölümleri için **forma doğrudan aktarılacak yeni metinleri** içerir. Değişikliklerin gerekçeleri ilgili ADR'lerde, sayısal hedefler ise [Proje Tüzüğü §4](../00-proje-tuzugu.md#4-başarı-ölçütleri-kpi) dosyasındadır. Formdaki her sayı o tablodan alınmalıdır.

> Formu doldurmadan önce: Başvurunun yapılacağı dönemin güncel şablonu TÜBİTAK'tan indirilmeli ve kapak sayfasındaki "2025 Yılı / 2025/1 Dönem" bilgisi güncellenmelidir.

## Değişiklik özeti

| Form bölümü | Sorun (eski taslak) | Yapılan değişiklik |
|---|---|---|
| Özet, 1.2, 2 | Ana model Isolation Forest + K-Means, "Spark MLlib ile Isolation Forest" (MLlib'de Isolation Forest yok) | Ana model DeepLog (LSTM) + Drain; Isolation Forest karşılaştırma (baseline) yöntemi ([ADR-0003](../adr/0003-deeplog-ana-model.md)) |
| Özet, 1.2, Şekil 1 | Apache Flume ve ZooKeeper | Çıkarıldı; Kafka KRaft modunda tek başına alımı üstleniyor ([ADR-0002](../adr/0002-veri-alimi.md)) |
| 2, Şekil 1 | "Spark Streaming" (eski DStream API'si) | Spark Structured Streaming 4.2 ([ADR-0004](../adr/0004-structured-streaming.md), [ADR-0007](../adr/0007-spark-4-ve-python-310.md)) |
| 1.2, 3.1, 3.2 | Çelişen hedefler: 500 / 1.000 / 10.000 log/sn; %85 / %90 "doğruluk"; 30 sn / 1 dk | Tek hedef tablosu (K1–K10); "doğruluk" yerine Precision/Recall |
| Şekil 2 | Arıza tipi ile aksiyon yer değiştirmiş; eşik sabit 0,85 | Disk dolu → Ansible disk temizleme, servis yanıt vermiyor → Docker yeniden başlatma; eşik doğrulama setinde seçilir |
| 1.1 | "Öngörü" iddiası var, ölçütü yok | Öngörü süresi (lead time) tanımlandı ve H2 hipotezi olarak test ediliyor |
| 2 | Model doğrulaması yalnızca sentetik hatalarla | Gerçek etiketli HDFS_v1 benchmark'ı (İz A) + demo sistemde arıza enjeksiyonu (İz B) ([ADR-0006](../adr/0006-veri-ve-degerlendirme.md)) |
| 2, Ön çalışma | "Saniyede 500 log aktarıldı" | Gerçekleştirilen ön çalışmanın ölçülmüş sonuçları |
| Ek-1 | Yıl/yazar hataları, tekrar eden kaynak, atıf yapılmayan kaynaklar | Kaynakça düzeltildi |

---

## Özet (yerine)

Bulut ve mikroservis mimarilerinin yaygınlaşması, sistemlerin ürettiği log ve metrik hacmini insanların izleyebileceği düzeyin çok üzerine çıkarmıştır. Geleneksel izleme araçları reaktiftir; alarm, kesinti yaşandıktan sonra gelir. Bu projenin amacı, dağıtık sistemlerden akan log ve metrik verisini gerçek zamanlı işleyen, anormallikleri arıza gerçekleşmeden yakalayan ve tanımlı senaryolarda sistemi insan müdahalesi olmadan toparlayan, tamamen açık kaynak bileşenlerden oluşan bir AIOps prototipi geliştirmektir.

Sistem Lambda mimarisi üzerine kuruludur. Veri Apache Kafka ile toplanır, Spark Structured Streaming ile Hadoop HDFS üzerindeki veri gölüne kayıpsız yazılır. Batch katmanında loglar Drain algoritmasıyla şablonlara ayrılır ve log dizilerinin sırasını öğrenen LSTM tabanlı DeepLog modeli eğitilir; Isolation Forest karşılaştırma yöntemi olarak kullanılır. Hız katmanı canlı akışı bu modelle skorlar ve eşik aşıldığında, ayrı bir servis olarak çalışan iyileştirme bileşeni Docker ve Ansible aracılığıyla disk temizleme ya da servis yeniden başlatma aksiyonu alır. Otonom aksiyonlar izin listesi, bekleme süresi ve acil durdurma anahtarı gibi korumalarla sınırlandırılmıştır.

Yöntem, gerçek etiketli Loghub HDFS_v1 veri setinde (575.061 oturum) Precision, Recall ve F1 ile; otonom döngü ise demo sistemde enjekte edilen arızalarla iyileştirme süresi, başarı oranı ve arızanın ne kadar önce sezildiği (öngörü süresi) üzerinden değerlendirilecektir. Proje sonunda TRL-4 düzeyinde çalışan bir prototip, açık kaynak kod deposu, lisans bitirme tezi ve ulusal bir kongre bildirisi üretilmesi hedeflenmektedir.

**Anahtar Kelimeler:** AIOps, Log Anomali Tespiti, DeepLog, Büyük Veri, Otonom İyileştirme

---

## 1.1 Literatür Özeti ve Bilimsel Nitelik (yerine)

Log tabanlı anomali tespiti üzerine yapılan çalışmalar (He vd., 2021; Landauer vd., 2023) iki eğilim göstermektedir. Birincisi, yöntemlerin çoğu geçmişe dönük statik veri setleri üzerinde değerlendirilmekte; canlı akış üzerindeki gecikme ve kaynak kullanımı raporlanmamaktadır. İkincisi, çalışmalar çoğunlukla "tespit" aşamasında kalmakta; tespitin otomatik bir iyileştirme aksiyonuna nasıl dönüştüğü ve bu dönüşümün sistem üzerindeki etkisi ölçülmemektedir. Kendi kendini iyileştiren sistemler literatürü (Ghosh vd., 2007) ise karar ve aksiyon tarafına odaklanmakta, öğrenilmiş anomali modelleriyle bütünleşik açık uygulamalar sunmamaktadır.

DeepLog (Du vd., 2017), sistem loglarını Drain (He vd., 2017) ile şablon kimliklerine dönüştürüp bu kimliklerin sırasını LSTM ile modelleyen ve etiket gerektirmeyen bir yöntemdir; HDFS_v1 üzerinde yüksek başarım raporlamıştır. Bu proje DeepLog'u Spark Structured Streaming (Armbrust vd., 2018) ile gerçek zamanlı çalıştırarak ve otonom bir aksiyon katmanına bağlayarak şu boşluğu hedeflemektedir: **log dizisi tabanlı bir derin öğrenme modelinin canlı akışta ürettiği alarmların, güvenlik korumalarıyla sınırlandırılmış otonom aksiyonlara dönüştürüldüğü, tamamen açık kaynak ve yeniden üretilebilir bir uçtan uca döngü** ve bu döngünün gecikme, başarı oranı ve öngörü süresi açısından ölçülmesi.

**Araştırma sorusu:** Dağıtık sistemlerden akan log verisi büyük veri teknolojileriyle gerçek zamanlı işlendiğinde, arızalar gerçekleşmeden ne kadar önce tespit edilebilir ve bu tespitlere dayanan otonom bir döngü servisi hangi başarı oranı ve sürede toparlayabilir?

**Hipotezler:**
- **H1:** DeepLog, HDFS_v1 test setinde Precision ≥ 0,90 ve Recall ≥ 0,90 sağlar ve Isolation Forest'tan istatistiksel olarak anlamlı ölçüde daha iyidir (McNemar testi, p < 0,05).
- **H2:** Kademeli gelişen arızalarda (disk dolması, bellek sızıntısı) alarm, servis arızası gerçekleşmeden önce üretilir; medyan öngörü süresi sıfırdan büyüktür (tek örneklem Wilcoxon testi).
- **H3:** Otonom döngü, enjekte edilen arızaların en az %90'ında servisi insan müdahalesi olmadan sağlıklı duruma döndürür; alarmdan aksiyonun tamamlanmasına kadar geçen süre p95 ≤ 30 sn'dir ve normal yükte gereksiz aksiyon üretmez.

> 12. Kalkınma Planı ve 2030 Sanayi ve Teknoloji Stratejisi ile ilişki paragrafları değişmeden kalabilir. Ancak "dakikalık kesintinin maliyeti" cümlesine kaynak eklenmelidir: Lerner (2014), Gartner, dakika başına yaklaşık 5.600 ABD doları.

---

## 1.2 Amaç ve Hedefler (yerine)

Projenin amacı, dağıtık sistemlerdeki arızaları log dizilerinden erken tespit eden ve tanımlı senaryolarda insan müdahalesi olmadan iyileştiren, açık kaynak bir AIOps prototipini (TRL-4) tasarlamak, geliştirmek ve ölçülebilir hedeflerle doğrulamaktır. Başarı ölçütleri şunlardır:

| No | Ölçüt | Hedef |
|---|---|---|
| K1 | Kafka → HDFS alım hızı | ≥ 5.000 log/sn, 10 dakika kesintisiz |
| K2 | Veri kaybı | %0 |
| K3 | Tespit başarısı (HDFS_v1 test seti) | Precision ≥ 0,90 ve Recall ≥ 0,90 |
| K4 | Karşılaştırma yöntemine üstünlük | DeepLog F1 > Isolation Forest F1, McNemar p < 0,05 |
| K5 | Tespit gecikmesi (log → alarm) | p95 ≤ 10 sn |
| K6 | İyileştirme süresi (alarm → aksiyon tamamlandı) | p95 ≤ 30 sn |
| K7 | Otonom iyileştirme başarısı | Enjekte edilen arızaların ≥ %90'ı |
| K8 | Gereksiz aksiyon | 2 saatlik normal yükte 0 |
| K9 | Öngörü süresi | Ölçülür ve raporlanır (H2) |
| K10 | Panel gecikmesi | p95 ≤ 2 sn |

---

## 2. Yöntem: değişen paragraflar

**Mimari (Şekil 1 açıklamasının yerine):** Sistem, Lambda mimarisine (Marz ve Warren, 2015) göre üç katmandan oluşur.
- **Alım:** Uygulama logları ve konteyner metrikleri (CPU, bellek, disk G/Ç, ağ) Apache Kafka'ya (Kreps vd., 2011; KRaft modu) gönderilir.
- **Depolama:** Spark Structured Streaming işi veriyi HDFS üzerindeki veri gölüne Parquet biçiminde, checkpoint mekanizmasıyla kayıpsız ve tekrarsız yazar; sözleşmeye uymayan kayıtları karantinaya ayırır.
- **Batch katmanı:** Logları Drain ile şablonlara ayırır, DeepLog modelini eğitir.
- **Hız katmanı:** Canlı akışı oturum bazında skorlar.
- **Aksiyon katmanı:** Alarmları tüketen ayrı bir servis, arıza tipini kurallarla belirleyip Docker veya Ansible ile aksiyon alır.
- **Sunum:** Sonuçlar React tabanlı bir panelde gösterilir.

**Şekil 1'de yapılacak değişiklikler:**
- Apache Flume kaldırılacak.
- "Spark Streaming" yerine "Spark Structured Streaming" yazılacak.
- "Betch Data" yazım hatası "Batch Data" olarak düzeltilecek.
- Aksiyon katmanına "Remediator (Ansible/Docker)" kutusu eklenecek.

**Otonom karar akışı (Şekil 2 açıklamasının yerine):**
1. Hız katmanı her oturum için 0 ile 1 arasında bir risk skoru üretir. Skor, son tahminlerde gerçekleşen olayın modelin ilk g adayı arasında olmadığı durumların oranıdır; bilinmeyen bir şablon görülürse skor 1 olur.
2. Eşik, sabit bir değer yerine doğrulama setinde F1'i en yüksek yapan değer olarak seçilir.
3. Eşik aşıldığında arıza tipi metriklerle birlikte belirlenir:
   - Disk doluluğu %90 ve üzerindeyse "disk dolu" kabul edilir; Ansible ile yalnızca izinli dizinler temizlenir.
   - Sağlık kontrolü üç kez üst üste başarısızsa "servis yanıt vermiyor" kabul edilir; Docker ile servis yeniden başlatılır.
   - Diğer durumlarda yalnızca bildirim yapılır.
4. Her aksiyon şu korumalardan geçer: izin listesi, aynı hedefe 5 dakikada en fazla 1 aksiyon, saatte en fazla 6 aksiyon, acil durdurma anahtarı.

**Şekil 2'de yapılacak değişiklikler:**
- "Sistem Cevap Vermiyor" dalı "Docker: Servisi Yeniden Başlat" kutusuna, "Disk Dolu" dalı "Ansible: Disk Temizle" kutusuna bağlanacak. Taslakta bu ikisi yer değiştirmişti.
- "Skor > 0.85" yerine "Skor > eşik (doğrulama setinde seçilir)" yazılacak.

**Veri ve değişkenler:**
- **İz A (bilimsel değerlendirme):** Gerçek bir Hadoop kümesinden toplanmış, etiketli Loghub HDFS_v1 veri seti kullanılır (Zhu vd., 2023): 11.175.629 log satırı ve 575.061 oturum; oturumların %2,93'ü anormal. Oturumlar zamana göre sıralanıp %60 eğitim (yalnızca normal oturumlar), %10 doğrulama ve %30 test olarak bölünür. Test seti yalnızca bir kez kullanılır.
- **İz B (otonom döngü):** Küçük bir demo uygulamada iki ani (süreç sonlandırma, yanıt vermeme) ve iki kademeli (disk dolması, bellek sızıntısı) arıza, her senaryo için 20 kez enjekte edilir.
- **Bağımsız değişkenler:** log hızı, pencere boyu (h), aday sayısı (g), eşik, arıza tipi, micro-batch süresi.
- **Bağımlı değişkenler:** Precision, Recall, F1, tespit gecikmesi, iyileştirme süresi, başarı oranı, öngörü süresi.

**İstatistiksel yöntemler:**
- Model karşılaştırması aynı test seti üzerinde McNemar testiyle yapılır.
- Başarı oranları %95 Wilson güven aralığıyla raporlanır.
- Öngörü süresinin sıfırdan büyük olduğu (H2) tek örneklem Wilcoxon testiyle sınanır.
- Model sonuçları 5 farklı rastgele tohumla tekrarlanır ve ortalama ± standart sapma olarak verilir.

---

## Ön Çalışma ve Fizibilite (yerine)

Proje önerisi öncesinde altyapı ve veri hattı tamamlanmış, aşağıdaki sonuçlar ölçülmüştür. Kodlar ve ölçüm betikleri açık kaynak olarak yayımlanmıştır (https://github.com/ardagonca/deeplog-aiops).

- **Altyapı:** Kafka 4.3 (KRaft), Hadoop HDFS 3.4 ve Spark 4.2, Docker Compose ile tek bir makinede kuruldu. Sistemin bütünlüğü, Kafka'dan Spark'a ve oradan HDFS'e uzanan otomatik bir uçtan uca testle doğrulanmaktadır.
- **Dayanıklılık:** Sürekli çalışan 8 servisin her biri süreci zorla sonlandırılarak (SIGKILL) çökertildi; hepsi kendiliğinden yeniden başladı ve 7–43 sn içinde hizmete döndü. Tüm servislerin aynı anda kapatıldığı ani kapanma senaryosunda sistem yaklaşık 60 sn'de geri geldi. Hiçbir senaryoda veri kaybı olmadı (17/17 kontrol).
- **Kayıpsız alım:** 150.003 kayıt yazılırken alım işi zorla sonlandırıldı. İş kendiliğinden yeniden başladı ve kaldığı yerden devam etti; Kafka'daki her kayıt HDFS'te tam olarak bir kez bulundu (kayıp 0, tekrar 0). Biçimi bozuk kayıtlar düşürülmeden, nedenleriyle birlikte karantinaya ayrıldı.
- **Metrik toplama:** Konteyner metrikleri, yalnızca okuma izni olan bir Docker API vekili üzerinden 5 sn aralıkla toplanmaktadır (ölçülen medyan aralık 5,0 sn, p95 5,06 sn).
- **Alım hızı (EXP-01):** _[Deney sürüyor; sonuçlar `experiments/EXP-01/` altındaki özetten aktarılacak.]_

---

## 3.1 Çalışma Takvimi (yerine)

Ay numaraları proje başlangıcına göredir. Başarı ölçütleri 1.2'deki tabloya atıf yapar.

| Tarih aralığı | Faaliyet | Kim | Başarı ölçütü | Katkı |
|---|---|---|---|---|
| Ay 1–2 | Kafka (KRaft), HDFS ve Spark'ın Docker üzerinde kurulumu, sağlık kontrolleri ve otomatik dayanıklılık testleri | Arda Gonca | Tüm servislerin sağlık kontrollerinden geçmesi; uçtan uca testin başarılı olması | %10 |
| Ay 2–4 | Log ve metrik toplama hattının (replayer, metrik ajanı, Structured Streaming ile HDFS'e yazma) geliştirilmesi | Arda Gonca | K1 ve K2 | %15 |
| Ay 4–7 | Drain ile log şablonlama, DeepLog modelinin geliştirilmesi ve Isolation Forest ile karşılaştırılması | Arda Gonca | K3 ve K4 | %25 |
| Ay 7–10 | Structured Streaming ile gerçek zamanlı tespit ile Docker/Ansible tabanlı otonom iyileştirme ve güvenlik korumaları | Arda Gonca | K5, K6, K7, K8; K9 ölçülür | %30 |
| Ay 10–12 | Web paneli, tüm modüllerin entegrasyonu ve uçtan uca nihai testler | Arda Gonca | K10 ve tüm ölçütlerin tek koşuda yeniden üretilmesi | %20 |

> Başvuru tarihinde tamamlanmış işler (yukarıdaki ilk iki satırın büyük bölümü) Ön Çalışma'ya taşınabilir; bu durumda katkı yüzdeleri toplamı 100 olacak şekilde yeniden dağıtılır.

---

## 3.2 Risk Yönetimi (yerine)

| En önemli riskler | Alınacak tedbirler (B planı) |
|---|---|
| Otonom aksiyonun yanlış alarmla zarar vermesi (gereksiz yeniden başlatma ya da veri silme) | Önlem olarak izin listesi, bekleme süresi, saatlik üst sınır, yalnızca izinli dizinleri temizleyen playbook ve acil durdurma anahtarı kullanılır. Gereksiz aksiyon görülürse aksiyonlar insan onaylı "öneri moduna" alınır ve sonuç (K8) olduğu gibi raporlanır. |
| Bilgisayar kaynaklarının (RAM, CPU) yetmemesi | Önlem olarak servis başına bellek sınırı ve yalnızca gereken servislerin açılması uygulanır. Yetmezse Spark tek konteynerde yerel modda çalıştırılır; bu da yetmezse sistem öğrenci kredisiyle bir bulut sanal makinesine taşınır. Mimari değişmez. |
| DeepLog'un K3 hedefine ulaşamaması | Önlem olarak makaleye sadık uygulama ve şablonlama doğruluğunun ayrıca ölçülmesi esas alınır. Hedefe ulaşılamazsa LogAnomaly veya Autoencoder tabanlı yöntemler denenir; yöntem değişikliği danışman onayıyla raporlanır. |
| Gerçek zamanlı skorlamada gecikmenin K5'i aşması | Önlem olarak micro-batch süresi ve bölüm sayısı ayarlanır. Gecikme sürerse skorlama Spark dışındaki bağımsız bir servise taşınır. Bu bir yöntem değişikliğidir: hız katmanının teknolojisi değişir, mimari aynı kalır. |
| Demo sistemdeki arızaların gerçek arızaları temsil etmemesi | Ani ve kademeli olmak üzere 4 senaryo kullanılır. Bu durum bir sınırlılık olarak raporlanır; ana bilimsel kanıt gerçek etiketli HDFS_v1 sonuçlarıdır. |

---

## 3.3 Araştırma Olanakları (eklenecek satır)

| Altyapı | Kullanım amacı |
|---|---|
| Yürütücünün kişisel bilgisayarı (32 GB RAM, 12 çekirdek; Windows 11 + WSL2/Docker) | Tüm altyapının ve deneylerin çalıştırılması; WSL2 için 12 GB ve 6 çekirdek ayrılmıştır |

---

## Ek-1: Kaynakça (düzeltilmiş)

Düzeltilen ve eklenen kaynaklar:
- **He vd. survey:** Metinde 2020 olarak geçen atıf 2021 olarak düzeltildi. İkinci yazarın adı "Pinjia, H." değil "He, P." olarak yazıldı.
- **Landauer vd. (2023):** Kaynakçada iki kez geçiyordu; tekrar kaldırıldı.
- **Blessing vd. (2025):** Yayın yeri belirtilmediği için çıkarıldı.
- **Pahl ve Jamshidi:** Künyesi doğrulanamadığı için çıkarıldı.
- **Eklenenler:** Zhu vd. (2023), Zhu vd. (2019), Armbrust vd. (2018), Lerner (2014). Tüm künyeler web üzerinden doğrulandı; ayrıntılar en alttaki "Doğrulama notları" bölümünde.

Armbrust, M., Das, T., Torres, J., Yavuz, B., Zhu, S., Xin, R., Ghodsi, A., Stoica, I., & Zaharia, M. (2018). Structured streaming: A declarative API for real-time applications in Apache Spark. In *Proceedings of the 2018 International Conference on Management of Data (SIGMOD '18)* (pp. 601–613). ACM. https://doi.org/10.1145/3183713.3190664

Chandola, V., Banerjee, A., & Kumar, V. (2009). Anomaly detection: A survey. *ACM Computing Surveys, 41*(3), Article 15. https://doi.org/10.1145/1541880.1541882

Du, M., Li, F., Zheng, G., & Srikumar, V. (2017). DeepLog: Anomaly detection and diagnosis from system logs through deep learning. In *Proceedings of the 2017 ACM SIGSAC Conference on Computer and Communications Security (CCS '17)* (pp. 1285–1298). ACM. https://doi.org/10.1145/3133956.3134015

Ghosh, D., Sharman, R., Rao, H. R., & Upadhyaya, S. (2007). Self-healing systems—Survey and synthesis. *Decision Support Systems, 42*(4), 2164–2185. https://doi.org/10.1016/j.dss.2006.06.011

He, P., Zhu, J., Zheng, Z., & Lyu, M. R. (2017). Drain: An online log parsing approach with fixed depth tree. In *2017 IEEE International Conference on Web Services (ICWS)* (pp. 33–40). IEEE. https://doi.org/10.1109/ICWS.2017.13

He, S., He, P., Chen, Z., Yang, T., Su, Y., & Lyu, M. R. (2021). A survey on automated log analysis for reliability engineering. *ACM Computing Surveys, 54*(6), Article 130. https://doi.org/10.1145/3460345

Kreps, J., Narkhede, N., & Rao, J. (2011). Kafka: A distributed messaging system for log processing. In *Proceedings of the 6th International Workshop on Networking Meets Databases (NetDB '11)*. Athens, Greece.

Landauer, M., Onder, S., Skopik, F., & Wurzenberger, M. (2023). Deep learning for anomaly detection in log data: A survey. *Machine Learning with Applications, 12*, Article 100470. https://doi.org/10.1016/j.mlwa.2023.100470

Lerner, A. (2014, July 16). *The cost of downtime*. Gartner Blog Network. https://blogs.gartner.com/andrew-lerner/2014/07/16/the-cost-of-downtime/

Liu, F. T., Ting, K. M., & Zhou, Z.-H. (2008). Isolation forest. In *2008 Eighth IEEE International Conference on Data Mining (ICDM)* (pp. 413–422). IEEE. https://doi.org/10.1109/ICDM.2008.17

Marz, N., & Warren, J. (2015). *Big Data: Principles and best practices of scalable realtime data systems*. Manning Publications.

Yazdanparast, Z. (2024). *A survey on self-healing software system* (arXiv:2403.00455). arXiv. https://doi.org/10.48550/arXiv.2403.00455

Zaharia, M., Chowdhury, M., Franklin, M. J., Shenker, S., & Stoica, I. (2010). Spark: Cluster computing with working sets. In *Proceedings of the 2nd USENIX Workshop on Hot Topics in Cloud Computing (HotCloud '10)*. USENIX Association.

Zhu, J., He, S., He, P., Liu, J., & Lyu, M. R. (2023). Loghub: A large collection of system log datasets for AI-driven log analytics. In *2023 IEEE 34th International Symposium on Software Reliability Engineering (ISSRE)* (pp. 355–366). IEEE. https://doi.org/10.1109/ISSRE59848.2023.00071

Zhu, J., He, S., Liu, J., He, P., Xie, Q., Zheng, Z., & Lyu, M. R. (2019). Tools and benchmarks for automated log parsing. In *2019 IEEE/ACM 41st International Conference on Software Engineering: Software Engineering in Practice (ICSE-SEIP)* (pp. 121–130). IEEE.

> Bifet vd. (2015, StreamDM) metinde kullanılmıyorsa kaynakçadan çıkarılabilir. Kaynakçadaki her kaynağa metinde en az bir atıf olmalıdır.

## Doğrulama notları

Künyeler 2026-11-18'de web'den doğrulandı. Metinde atıf yapılmayan kaynaklar: Chandola vd. (2009), Liu vd. (2008), Yazdanparast (2024), Zaharia vd. (2010) ve eklenen Zhu vd. (2019). Bunlara metinde atıf eklenmeli ya da kaynakçadan çıkarılmalıdır. Zhu vd. (2019), ön çalışmadaki şablonlama doğruluğu ölçütünün (Grouping Accuracy) kaynağıdır; Ön Çalışma bölümünde bu ölçüt kullanılırsa atıf orada yapılmalıdır.

| Kaynak | Durum | Bağlantı |
|---|---|---|
| Armbrust vd. (2018) | Doğrulandı; sayfa (601–613) doğru, DOI eklendi | https://dblp1.uni-trier.de/rec/conf/sigmod/ArmbrustDTYZX0S18.html |
| Chandola vd. (2009) | Düzeltildi: "1–58" yerine Article 15 (58 sayfa), DOI eklendi | https://www.profsandhu.com/cs5323_s17/a15-chandola.pdf |
| Du vd. (2017) | Doğrulandı; sayfa (1285–1298) doğru, DOI eklendi | https://dblp.org/rec/conf/ccs/Du0ZS17 |
| Ghosh vd. (2007) | Doğrulandı; DOI eklendi | https://dblp.org/rec/journals/dss/GhoshSRU07 |
| He vd. (2017) | Doğrulandı; DOI eklendi | https://researchr.org/publication/HeZZL17 |
| He vd. (2021) | Düzeltildi: "1–37" yerine Article 130, DOI eklendi | https://arxiv.org/abs/2009.07237v2 |
| Kreps vd. (2011) | Düzeltildi: yayın yeri tam adıyla yazıldı (NetDB '11, Atina). Sayfa aralığı (1–7) resmi bir kaynakta doğrulanamadığı için çıkarıldı | https://people.csail.mit.edu/matei/courses/2015/6.S897/readings/kafka.pdf |
| Landauer vd. (2023) | Doğrulandı; Article 100470, DOI eklendi | https://doaj.org/article/50b62d84af08411ebc50e4fa37f10fb6 |
| Lerner (2014) | Düzeltildi: tarih ve bağlantı eklendi. 5.600 $/dk rakamı ikincil kaynaklarda doğrulandı; blog yazısının kendisine erişilemedi | https://blogs.gartner.com/andrew-lerner/2014/07/16/the-cost-of-downtime/ |
| Liu vd. (2008) | Doğrulandı; sayfa (413–422) doğru, DOI eklendi | https://researchr.org/publication/LiuTZ08/bibtex |
| Marz ve Warren (2015) | Doğrulandı (Manning, ISBN 9781617290343) | https://www.manning.com/books/big-data |
| Yazdanparast (2024) | Doğrulandı (arXiv:2403.00455, yalnızca v1) | https://arxiv.org/abs/2403.00455v1 |
| Zaharia vd. (2010) | Düzeltildi: "HotCloud, 10, 95" gerçek bir cilt/sayfa değil; yayın yeri HotCloud '10 olarak yazıldı | https://usenix.org/conference/hotcloud-10/spark-cluster-computing-working-sets |
| Zhu vd. (2023) | Doğrulandı; sayfa (355–366) doğru, DOI eklendi | https://arxiv.org/abs/2008.06448 |
| Zhu vd. (2019) | Eklendi; ICSE-SEIP 2019, 121–130 | https://researchr.org/publication/ZhuHLHXZL19 |
