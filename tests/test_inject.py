"""REM-02: arıza anı, probe'un ilk başarısız olduğu isteğin gönderilme anıdır."""

from demo_target.inject import wait_failure


class FakeClock:
    def __init__(self):
        self.t = 100.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.t += s


def test_failure_time_is_start_of_first_failing_probe():
    clock = FakeClock()
    results = iter([True, True, False])

    def check() -> bool:
        ok = next(results)
        clock.t += 0.3 if ok else 2.0  # başarısız istek zaman aşımına kadar sürer
        return ok

    assert wait_failure(check, timeout=10, clock=clock, sleep=clock.sleep) == 102.0


def test_no_failure_within_timeout_returns_none():
    clock = FakeClock()
    assert wait_failure(lambda: True, timeout=5, clock=clock, sleep=clock.sleep) is None
    assert clock.t >= 105
