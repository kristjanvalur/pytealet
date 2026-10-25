"""Parser and summary for the Bencher HTTP bench. No server, no wrk."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "tealetio_bench_ci_http",
    Path(__file__).resolve().parents[1] / "bench" / "ci_http.py",
)
assert _SPEC is not None and _SPEC.loader is not None
ci_http = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = ci_http
_SPEC.loader.exec_module(ci_http)


CLEAN = """\
Running 10s test @ http://127.0.0.1:8080/
  4 threads and 256 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    24.83ms   12.10ms  48.00ms   68.22%
    Req/Sec     2.45k   300.12     3.10k    70.00%
  Latency Distribution
     50%   24.83ms
     75%   32.00ms
     90%   40.00ms
     99%   43.20ms
  97000 requests in 10.00s, 40.00MB read
Requests/sec:   9700.12
Transfer/sec:      4.00MB
"""

OUTLIER = """\
  Latency Distribution
     50%   16.37ms
     99%    1.36s
  427 requests in 2.44s, 747.40KB read
Requests/sec:    175.36
"""

ERRORS = """\
  Socket errors: connect 0, read 0, write 0, timeout 12
  Non-2xx or 3xx responses: 3
  Latency Distribution
     99%   40.00ms
Requests/sec:   1000.00
"""


def test_parse_clean_report() -> None:
    sample = ci_http.parse_wrk_output(CLEAN)
    assert sample["req_sec"] == 9700.12
    assert sample["p99_ms"] == 43.20
    assert ci_http.reject_reason(sample) is None


def test_latency_units_and_outlier() -> None:
    assert ci_http.latency_ms(850.0, "us") == 0.85
    sample = ci_http.parse_wrk_output(OUTLIER)
    assert sample["p99_ms"] == 1360.0
    assert ci_http.reject_reason(sample) == "p99 1360 ms"


def test_socket_errors_and_non_2xx() -> None:
    sample = ci_http.parse_wrk_output(ERRORS)
    assert ci_http.reject_reason(sample) == "socket errors (12)"
    sample["socket_errors"] = 0
    assert ci_http.reject_reason(sample) == "non-2xx responses (3)"
    sample["non_2xx"] = 0
    sample["req_sec"] = 0
    assert ci_http.reject_reason(sample) == "no completed requests"


def test_median_and_measures() -> None:
    assert ci_http.median([10.0, 1.0, 4.0]) == 4.0
    assert ci_http.median([1.0, 4.0]) == 2.5
    measures = ci_http.measures_for(
        [
            {"req_sec": 100.0, "p99_ms": 30.0},
            {"req_sec": 300.0, "p99_ms": 10.0},
            {"req_sec": 200.0, "p99_ms": 20.0},
        ]
    )
    assert measures["throughput"] == {"value": 200.0, "lower_value": 100.0, "upper_value": 300.0}
    assert measures["p99_ms"]["value"] == 20.0


def test_append_and_summary(tmp_path: Path) -> None:
    path = tmp_path / "bench-results.json"
    ci_http._append_result(path, "http-sync-3.15", ci_http.measures_for([{"req_sec": 9000.0, "p99_ms": 40.0}]))
    ci_http._append_result(path, "http-threaded-3.15t", ci_http.measures_for([{"req_sec": 15000.0, "p99_ms": 15.0}]))
    data = json.loads(path.read_text())
    assert set(data) == {"http-sync-3.15", "http-threaded-3.15t"}
    text = ci_http.format_summary(data)
    assert "`http-sync-3.15`" in text
    assert "9000" in text
    assert "15000" in text
    assert "40.0" in text
