"""REM-04/06: Remediator'ın karar çekirdeği: arıza tipi kuralları ve korumalar (ADR-0005, ADR-0011).

Kafka'dan ve Docker'dan bağımsızdır; remediation.service her alarm için decide() çağırır, dönen
karara göre aksiyonu çalıştırır ve sonucu actions topic'ine yazar.

Kurallar (sıra önemli; ilk uyan):
1. disk_used_pct >= DISK_PCT                     -> disk_full            -> ansible:disk_cleanup
2. mem_bytes / mem_limit_bytes >= MEM_RATIO      -> memory_exhaustion    -> docker:restart
3. ardışık /health başarısızlığı >= HEALTH_FAILS -> service_unresponsive -> docker:restart
4. diğer                                         -> unknown              -> none
Kademeli arızalarda ilk alarmlar eşikten önce gelir (erken uyarı) ve unknown kalır; arıza
ilerledikçe gelen alarmlar kural eşiğine ulaşınca aksiyon alınır.

Korumalar (sıra): tekrar eden alarm, kill switch, allowlist, bilinmeyen tip, cooldown, saatlik
üst sınır. Bilinmeyen tip cooldown ve saatlik sınırı tüketmez.
"""

from collections import deque
from dataclasses import dataclass

DISK_PCT = 90.0
MEM_RATIO = 0.9
HEALTH_FAILS = 3
COOLDOWN_S = 300.0
MAX_PER_HOUR = 6


@dataclass
class TargetState:
    """Remediator'ın hedef hakkında bildiği son durum (metrics.raw ve kendi /health yoklaması)."""

    disk_used_pct: float | None = None
    mem_bytes: int | None = None
    mem_limit_bytes: int | None = None
    health_failures: int = 0  # ardışık başarısız /health yoklaması


@dataclass(frozen=True)
class Decision:
    fault_type: str
    action: str
    status: str | None  # None: aksiyon çalıştırılacak; değilse atlama nedeni (ActionEvent.STATUSES)


def classify(s: TargetState) -> tuple[str, str]:
    if s.disk_used_pct is not None and s.disk_used_pct >= DISK_PCT:
        return "disk_full", "ansible:disk_cleanup"
    if (
        s.mem_bytes is not None
        and s.mem_limit_bytes
        and s.mem_bytes / s.mem_limit_bytes >= MEM_RATIO
    ):
        return "memory_exhaustion", "docker:restart"
    if s.health_failures >= HEALTH_FAILS:
        return "service_unresponsive", "docker:restart"
    return "unknown", "none"


class Guards:
    def __init__(
        self,
        enabled: bool,
        allowlist: set[str],
        cooldown_s: float = COOLDOWN_S,
        max_per_hour: int = MAX_PER_HOUR,
        remember: int = 100_000,
    ):
        self.enabled, self.allowlist = enabled, allowlist
        self.cooldown_s, self.max_per_hour = cooldown_s, max_per_hour
        self._seen: set[str] = set()
        self._order: deque[str] = deque()  # en eski kimlik önce unutulur (bellek sınırı)
        self._remember = remember
        self._last: dict[str, float] = {}
        self._recent: deque[float] = deque()

    def _first_time(self, alert_id: str) -> bool:
        if alert_id in self._seen:
            return False
        self._seen.add(alert_id)
        self._order.append(alert_id)
        if len(self._order) > self._remember:
            self._seen.discard(self._order.popleft())
        return True

    def check(self, alert_id: str, target: str | None, action: str, now: float) -> str | None:
        """Aksiyon alınabilirse None; alınamazsa atlama nedeni. Alınacaksa kullanımı kaydeder."""
        if not self._first_time(alert_id):
            return "skipped_duplicate"
        if not self.enabled:
            return "skipped_disabled"
        if target not in self.allowlist:
            return "skipped_not_allowlisted"
        if action == "none":
            return "skipped_unknown"
        if now - self._last.get(target, float("-inf")) < self.cooldown_s:
            return "skipped_cooldown"
        while self._recent and now - self._recent[0] >= 3600:
            self._recent.popleft()
        if len(self._recent) >= self.max_per_hour:
            return "skipped_rate_limit"
        self._last[target] = now
        self._recent.append(now)
        return None


def decide(
    guards: Guards, alert_id: str, target: str | None, state: TargetState | None, now: float
) -> Decision:
    fault_type, action = classify(state) if state is not None else ("unknown", "none")
    return Decision(fault_type, action, guards.check(alert_id, target, action, now))
