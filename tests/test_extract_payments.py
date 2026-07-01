from __future__ import annotations

import datetime as dt
import decimal

from snowflake_etl.src import extract_payments as module


class _FakeCursor:
    """Stands in for a psycopg2 server-side (named) cursor."""

    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.itersize: int | None = None
        self.executed: str | None = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, sql: str) -> None:
        self.executed = sql

    def __iter__(self):  # noqa: ANN204
        return iter(self._rows)


class _FakeConn:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self.last_cursor: _FakeCursor | None = None
        self.closed = False

    def cursor(self, name: str | None = None) -> _FakeCursor:
        self.last_cursor = _FakeCursor(self._rows)
        return self.last_cursor

    def close(self) -> None:
        self.closed = True


def _row(payment_id: int, currency: str) -> tuple:
    return (
        payment_id, 10, 700, decimal.Decimal("149.99"), currency,
        "card", "authorized", "US", dt.datetime(2025, 6, 30), dt.datetime(2025, 6, 30),
    )


def test_fetch_payments_yields_dicts_via_named_cursor() -> None:
    conn = _FakeConn([_row(1, "EUR"), _row(2, "USD")])

    rows = list(module.fetch_payments(conn, itersize=123))

    assert [r["payment_id"] for r in rows] == [1, 2]
    assert rows[0]["currency"] == "EUR"
    assert set(rows[0]) == set(module.PAYMENT_COLUMNS)  # every column mapped
    assert conn.last_cursor.itersize == 123               # server-side batch size applied
    assert "FROM payments ORDER BY payment_id" in conn.last_cursor.executed


def test_connect_from_env_reads_pg_env(monkeypatch) -> None:  # noqa: ANN001
    captured: dict = {}
    monkeypatch.setattr(module.psycopg2, "connect", lambda **kw: captured.update(kw) or "CONN")
    monkeypatch.setenv("PGHOST", "db-host")
    monkeypatch.setenv("PGPORT", "6543")
    monkeypatch.setenv("POSTGRES_DB", "payments")
    monkeypatch.setenv("POSTGRES_USER", "dataeng")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

    assert module.connect_from_env() == "CONN"
    assert captured["host"] == "db-host"
    assert captured["port"] == 6543
    assert captured["dbname"] == "payments"
    assert captured["user"] == "dataeng"


def test_main_dry_run_counts_and_samples(monkeypatch, capsys) -> None:  # noqa: ANN001
    conn = _FakeConn([_row(i, "EUR") for i in range(1, 6)])
    monkeypatch.setattr(module, "connect_from_env", lambda: conn)

    count = module.main(["--dry-run"])

    assert count == 5
    assert conn.closed is True  # connection always closed
    assert "payment_id" in capsys.readouterr().out  # sample rows printed
