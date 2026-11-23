"""RT-05: tespit işi etiketi (label) okumamalı; etiket yalnızca değerlendirme içindir."""

import re
from pathlib import Path

SOURCE = (Path(__file__).parents[1] / "jobs" / "detect.py").read_text(encoding="utf-8")


def test_detect_job_never_selects_label():
    code = SOURCE.split('"""', 2)[2]  # modül docstring'i hariç
    assert not re.search(r"\blabel\b", code)
