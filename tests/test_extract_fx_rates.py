from __future__ import annotations

import datetime as dt

import pytest

from snowflake_etl.src import extract_fx_rates as module


def _two_day_payload() -> dict:
    # Frankfurter shape: rates are "currency per 1 USD" (from=USD).
    return {
        "base": "USD",
        "rates": {
            "2025-06-30": {"EUR": 0.90, "GBP": 0.80},
            "2025-07-01": {"EUR": 0.95, "GBP": 0.82},
        },
    }


def test_fetch_builds_url_and_params_and_inverts_to_usd() -> None:
    captured: dict = {}

    def fake_getter(url: str, params: dict) -> dict:
        captured["url"] = url
        captured["params"] = params
        return _two_day_payload()

    rates = module.fetch_fx_rates(
        "2025-06-30", "2025-07-01", quotes=("EUR", "GBP"), getter=fake_getter
    )

    # Time-series URL + single-call params for all quote currencies.
    assert captured["url"].endswith("/2025-06-30..2025-07-01")
    assert captured["params"] == {"from": "USD", "to": "EUR,GBP"}

    # USD identity row added once per date.
    usd_rows = [r for r in rates if r.currency == "USD"]
    assert len(usd_rows) == 2
    assert all(r.rate_to_usd == 1.0 for r in usd_rows)

    # The inversion: API gives EUR=0.90 per USD -> rate_to_usd = 1/0.90.
    eur = next(r for r in rates if r.currency == "EUR" and r.rate_date == "2025-06-30")
    assert eur.rate_to_usd == round(1.0 / 0.90, 8)


def test_fetch_skips_null_rates() -> None:
    def getter(url: str, params: dict) -> dict:
        return {"rates": {"2025-06-30": {"EUR": None, "GBP": 0.80}}}

    rates = module.fetch_fx_rates("a", "b", quotes=("EUR", "GBP"), getter=getter)

    assert not any(r.currency == "EUR" for r in rates)   # null dropped
    assert any(r.currency == "GBP" for r in rates)       # valid kept


class _Resp:
    def __init__(self, status_code: int, body: dict | None = None) -> None:
        self.status_code = status_code
        self._body = body or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise module.requests.HTTPError(str(self.status_code))

    def json(self) -> dict:
        return self._body


def test_retry_recovers_after_transient_5xx(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def fake_get(url, params=None, timeout=None):  # noqa: ANN001
        calls["n"] += 1
        return _Resp(503) if calls["n"] == 1 else _Resp(200, {"rates": {}})

    monkeypatch.setattr(module.requests, "get", fake_get)
    monkeypatch.setattr(module.time, "sleep", lambda *_: None)  # don't actually wait

    assert module._get_json_with_retry("u", {}, base_delay=0) == {"rates": {}}
    assert calls["n"] == 2  # one failure, one success


def test_retry_raises_after_max_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module.requests, "get", lambda *a, **k: _Resp(500))
    monkeypatch.setattr(module.time, "sleep", lambda *_: None)

    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        module._get_json_with_retry("u", {}, max_attempts=3, base_delay=0)


def test_default_window_spans_365_days() -> None:
    start, end = module.default_window(dt.date(2026, 6, 29))
    assert (start, end) == ("2025-06-29", "2026-06-29")
